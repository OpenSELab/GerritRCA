from typing import Dict, List, Optional, Tuple
import re

from loguru import logger

import configs
from core.llm.zte_llm import ZteLLMClient
from core.log_template.parsing_cache import template_generating
from core.prompts.prompt_loader import PromptLoader
from core.utils import response_extractor


class LogTrackingAgent:
    """
    日志追踪 Agent

    提供三类核心能力：
    1) 信息输入：支持 LLM 抽取 / 日志模板匹配两种方式
    2) 信息更新：基于模板解析结果增量更新动态信息
    3) 日志追踪：基于动态信息在解析结果中追踪相关日志
    """

    def __init__(self, *_args, llm_config: Optional[dict] = None):
        self._content: Dict[str, str] = {}
        self._llm = ZteLLMClient(llm_config or configs.LLM_CONFIG)
        self._complement_info = None

    def to_dict(self):
        return {
            "content": self._content,
            "_llm": str(self._llm),
        }

    def add_complement_path(self, complement_info):
        self._complement_info = complement_info

    # ===== ① 信息输入 =====
    def input_info_by_llm(self, desc, orgin):
        """
        使用大模型从描述中提取动态信息并写入内容池。
        """
        json_reply = self._llm.infer(
            system_prompt="你是一个信息提取专家",
            user_prompt=PromptLoader.get_prompt(
                prompt_name="localization/dynamic_info_extractor.prompt",
                description=desc,
                orgin=orgin,
                dynamic_list=self._content,
            ),
        )
        result = response_extractor(json_reply)

        for item in result.get("result", []):
            for k, v in item.items():
                if k not in self._content:
                    self._content[k] = v

        logger.info(f"动态信息新增完成，现有动态信息为：{self._content}")

    def input_info_by_template(
        self, log_lines: List[str], template_name: str, auto_update: bool = False
    ) -> Tuple[List[str], List[List[str]], List[List]]:
        """
        使用模板匹配进行信息输入与日志初筛。
        """
        logger.info(f"开始通过日志模板解析定位日志，模板名: {template_name}")
        logger.info(f"当前动态信息: {self._content}")
        logger.info("开始生成日志模板...")

        try:
            template_list, parse_result = template_generating(
                llm_config=configs.LLM_CONFIG,
                log_file_line=log_lines,
                template_name=template_name,
            )
            logger.info("日志模板生成完成")
            logger.info(f"模板列表长度: {len(template_list)}")
            logger.info(f"解析结果长度: {len(parse_result)}")
        except Exception as e:
            logger.error(f"生成日志模板失败: {e}")
            return [], [], []

        matched_logs = self._trace_logs_from_template_result(template_list, parse_result)
        logger.info(f"最终匹配到 {len(matched_logs)} 条日志")

        if auto_update and matched_logs:
            self.update_info(matched_logs, template_list, parse_result)

        return matched_logs, template_list, parse_result

    # ===== ② 信息更新 =====
    def update_info(self, matched_logs: List[str], template_list: List[List[str]], parse_result: List[List]):
        """
        从模板解析结果更新动态信息。
        """
        logger.info("开始从模板解析结果更新动态信息管理器...")

        template_placeholder_map = self._build_template_placeholder_map(template_list)
        log_to_parse_map = {}
        for parse_item in parse_result:
            if len(parse_item) >= 2:
                original_log = parse_item[0]
                log_to_parse_map[original_log] = parse_item

        new_items_count = 0
        for matched_log in matched_logs:
            if matched_log not in log_to_parse_map:
                continue

            parse_item = log_to_parse_map[matched_log]
            template = parse_item[1]
            parameters = parse_item[2:] if len(parse_item) > 2 else []
            placeholders = template_placeholder_map.get(template, [])

            for idx, param_value in enumerate(parameters):
                if idx >= len(placeholders):
                    continue
                placeholder_desc = placeholders[idx]
                if placeholder_desc == "-1":
                    continue
                param_value_str = str(param_value)
                if param_value_str not in self._content:
                    self._content[param_value_str] = placeholder_desc
                    new_items_count += 1
                    logger.debug(f"添加新的动态信息: {param_value_str} -> {placeholder_desc}")

        logger.info(f"动态信息管理器更新完成，新增 {new_items_count} 条动态信息")
        return matched_logs

    # ===== ③ 日志追踪 =====
    def trace_logs(self, template_list: List[List[str]], parse_result: List[List], auto_update: bool = True):
        """
        使用已解析模板结果进行日志追踪，避免重复解析。
        """
        logger.info("开始使用已解析的模板结果进行日志定位")
        logger.info(f"当前动态信息: {self._content}")

        matched_logs = self._trace_logs_from_template_result(template_list, parse_result)
        logger.info(f"使用已解析模板结果匹配到 {len(matched_logs)} 条日志")

        if auto_update and matched_logs:
            self.update_info(matched_logs, template_list, parse_result)

        return matched_logs

    def _build_template_placeholder_map(self, template_list: List[List[str]]) -> Dict[str, List[str]]:
        template_placeholder_map: Dict[str, List[str]] = {}
        for template_row in template_list:
            if len(template_row) > 0:
                template = template_row[0]
                placeholders = template_row[1:] if len(template_row) > 1 else []
                template_placeholder_map[template] = placeholders
        return template_placeholder_map

    def _trace_logs_from_template_result(self, template_list: List[List[str]], parse_result: List[List]):
        template_placeholder_map = self._build_template_placeholder_map(template_list)
        matched_logs = []

        for parse_item in parse_result:
            if len(parse_item) < 2:
                continue
            original_log = parse_item[0]
            template = parse_item[1]
            parameters = parse_item[2:] if len(parse_item) > 2 else []
            placeholders = template_placeholder_map.get(template, [])

            matched = False
            for idx, param_value in enumerate(parameters):
                if idx >= len(placeholders):
                    continue
                placeholder_desc = placeholders[idx]
                if placeholder_desc == "-1":
                    continue
                param_value_str = str(param_value)
                if param_value_str in self._content:
                    matched = True
                    logger.info(
                        f"匹配成功: 日志={original_log[:50]}..., 模板={template}, 参数位置={idx}, 值={param_value_str}"
                    )
                    break

            if matched:
                matched_logs.append(original_log)
        return matched_logs

    def get_content(self):
        return self._content

    def tokenize(self, text: str) -> set:
        tokens = re.split(r"(?<!-)(?<!\()[\s./]+(?!-)", text)
        return set(filter(None, tokens))

    # ===== 向后兼容旧接口 =====
    def add(self, desc, orgin):
        self.input_info_by_llm(desc=desc, orgin=orgin)

    def query_related_log_line_by_log_template(self, log_lines: List[str], template_name: str):
        matched_logs, template_list, parse_result = self.input_info_by_template(
            log_lines=log_lines,
            template_name=template_name,
            auto_update=False,
        )
        if matched_logs:
            self.update_info(matched_logs, template_list, parse_result)
        return matched_logs

    def query_related_log_line_by_log_template_with_result(self, log_lines: List[str], template_name: str):
        return self.input_info_by_template(
            log_lines=log_lines,
            template_name=template_name,
            auto_update=False,
        )

    def query_related_log_line_by_parsed_result(self, template_list: List[List[str]], parse_result: List[List]):
        return self.trace_logs(template_list=template_list, parse_result=parse_result, auto_update=True)

    def update_from_template_result(self, matched_logs: List[str], template_list: List[List[str]], parse_result: List[List]):
        return self.update_info(
            matched_logs=matched_logs,
            template_list=template_list,
            parse_result=parse_result,
        )

