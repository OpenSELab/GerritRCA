# coding=utf-8
from ast import literal_eval

import pandas as pd
from loguru import logger

import configs
from core.llm.base import LLMClient
from core.localization.log_tracking_agent import LogTrackingAgent
from core.log_template.log_precondition import log_templates_parsing
from core.prompts.prompt_loader import PromptLoader
from core.utils import response_extractor


class RootCaseDetector:
    """
    根因分析器
    """

    def __init__(
        self,
        online_log,
        log_name,
        step_row,
        job,
        pre_root_case,
        dynamic,
        llm_config,
    ):
        self.online_log = online_log
        self.component = step_row
        self.log_name = log_name
        self.llm = LLMClient(llm_config)
        self.job = job
        self.pre_root_case = pre_root_case
        self.dynamic = dynamic

    def analyze(self):
        """
        进行阶段根因分析
        """
        result = response_extractor(
            self.llm.infer(
                system_prompt="你是一个系统故障根因分析专家",
                user_prompt=PromptLoader.get_prompt(
                    prompt_name="root_case/stage_analysis.prompt",
                    job=self.job.get("job"),
                    job_description=self.job.get("job_descripttion"),
                    check_seq=self.job.get("check_seq"),
                    pre_root_case=self.job.get("pre_root_case"),
                    component=self.log_name,
                    component_duty=self.component["description"].values[0],
                    online_log=self.online_log,
                    dynamic=self.dynamic,
                    prompt_message=self.component["prompt_message"].values[0],
                ),
            )
        ).get("result")
        check_result = result.get("result")
        return check_result

    def summary(
        self,
        stage_analysis,
        log_info,
        fault_description,
        detect_chain_description,
    ):
        """
        对阶段性根因分析进行总结
        """
        result = response_extractor(
            self.llm.infer(
                system_prompt="你是一个系统故障根因分析与总结专家",
                user_prompt=PromptLoader.get_prompt(
                    prompt_name="root_case/whole.prompt",
                    job=self.job.get("job"),
                    job_description=self.job.get("job_descripttion"),
                    check_seq=self.job.get("check_seq"),
                    pre_root_case=stage_analysis,
                    log_info=self.log_name,
                    fault_description=fault_description,
                    detect_chain_description=detect_chain_description,
                ),
            )
        ).get("result")
        reply = "\n".join(str(v) for v in result.values())
        return reply


class ChechFlowGenerator:
    """
    故障排查路径生成器
    """

    def __init__(self, row, df_description_map, job_name, description, dynamic):
        self.llm = LLMClient(configs.DEEPSEEK_CONFIG)
        self.key_row = row
        self.file_map = df_description_map
        self.job = job_name
        self.fault_description = description
        self.dynamic = dynamic

    def generate_check_flow(self):
        """
        生成故障排查路径并检查
        """
        extra_file = []
        file_names = self.file_map["file_name"].values
        assistant = []
        description, chain, response = self.llm.infer_with_assistant(
            system_prompt="你是一个故障处理专家",
            user_prompt=PromptLoader.get_prompt(
                prompt_name="preprocessing/workflow_generating.prompt",
                job_name=self.job,
                job_description=self.key_row["workchain"].values[0],
                prompt_message=self.key_row["prompt_message"].values[0],
                fault_description=self.fault_description,
                calling_seq=self.key_row["prompt_message"].values[0],
                log_file_information=self.file_map,
                dynamic=self.dynamic,
            ),
            assistant=[],
        )
        assistant += response
        while True:
            valid = True
            extra_file = []
            for s in chain:
                if s[-2] not in file_names:
                    valid = False
                    extra_file.append(s[-2])
            if valid:
                logger.info("生成日志文件合法，生成结果如下：")
                break
            logger.info("有不存在默认排查顺序的日志文件，开始验证...")
            logger.info(f"不存在的日志文件：{str(extra_file)}")
            logger.info("================")
            description, chain, ass = self.llm.infer_with_assistant(
                system_prompt="",
                user_prompt=PromptLoader.get_prompt(
                    "preprocessing/workflow_check.prompt",
                    extra_file=extra_file,
                    calling_seq=self.key_row["calling_seq"].values[0],
                ),
                assistant=assistant,
            )
            assistant += ass
        return description, chain


class WorkFlowDivider:
    """
    根因分析主流程
    """

    def __init__(self, job_name: str, fault_description: str, llm_config):
        self.df = None
        self.job_name = job_name
        self.fault_description = fault_description
        self.llm_config = llm_config
        self.llm = LLMClient(self.llm_config)
        self.di = LogTrackingAgent()
        self.sf = None

    def build_detect_chain(self, file_description_map_path: str, row: pd.Series, dynamic):
        """
        对外统一接口：与 run.py 保持一致，返回 (result, detect_chain)。
        """
        return self.check(
            file_description_map_path=file_description_map_path,
            row=row,
            dynamic=dynamic,
        )

    def check(self, file_description_map_path: str, row: pd.Series, dynamic):
        """
        根据输入的全体日志描述、正常流程和动态信息进行根因分析
        """
        df_map = pd.read_csv(file_description_map_path, encoding="utf-8")
        self.di.add(desc=dynamic, orgin="初始信息")

        logger.info("正在生成故障排查路径...")
        cfg = ChechFlowGenerator(
            row=row,
            df_description_map=df_map,
            job_name=self.job_name,
            description=self.fault_description,
            dynamic=dynamic,
        )
        detect_chain_description, detect_chain = cfg.generate_check_flow()

        logger.info(f"故障排查路径为：{detect_chain}")
        logger.info(f"{detect_chain_description}")
        file_analysis_results = []
        count = 1

        for step in detect_chain:
            platform = ""
            step_row = pd.Series(dtype=object, index=df_map.columns)
            if len(step) == 4:
                platform = f"{step[0]}/{step[1]}/{step[2]}"
                single_file_description = step[3]
                logger.info("################################################")
                logger.info(
                    f"开始第{count}个日志文件:{platform}的排查，对应具体文件:{single_file_description}"
                )
                mask = (
                    (df_map["platform"] == step[0])
                    & (df_map["subsidiary_platform"] == step[1])
                    & (df_map["file_name"] == step[2])
                )
                step_row = df_map[mask]
            elif len(step) == 3:
                platform = f"{step[0]}/{step[1]}"
                single_file_description = step[2]
                logger.info("################################################")
                logger.info(
                    f"开始第{count}个日志文件:{platform}的排查，对应具体文件:{single_file_description}"
                )
                mask = (df_map["platform"] == step[0]) & (df_map["file_name"] == step[1])
                step_row = df_map[mask]
            else:
                logger.error("文件信息录入有误，请检查resource/map.csv中的文件内容")
                return None, detect_chain
            count += 1

            prefix_parameters = literal_eval(step_row["prefix_parameters"].values[0])

            logger.info(f"开始读取日志文件: {step_row['path'].values[0]}")
            try:
                with open(step_row["path"].values[0], "r", encoding="utf-8") as f:
                    all_log_lines = f.readlines()
                logger.info(f"读取完成，共 {len(all_log_lines)} 条日志")
            except Exception as e:
                logger.error(f"读取日志文件失败: {e}")
                all_log_lines = []

            if len(all_log_lines) == 0:
                logger.warning("日志文件为空，跳过当前日志文件的排查，直接进行根因分析")
                contents = []
                template_list = []
                parse_result = []
                df_log = pd.DataFrame()
            else:
                logger.info("开始对日志进行解析...")
                logs = log_templates_parsing(
                    key_lines=all_log_lines,
                    prefix_format=step_row["prefix_format"].values[0],
                    llm_config=self.llm_config,
                    prefix_parameters=prefix_parameters,
                    is_pasered=step_row["is_parsered"].values[0],
                )
                df_log = pd.DataFrame(logs)

                flag = not (pd.isna(dynamic.get("start_time")) or pd.isna(dynamic.get("end_time")))

                if len(logs) > 0 and (
                    (logs[0].get("timestamp") is None or (step_row["div"].values[0] == 0))
                    or not flag
                ):
                    logger.info("日志解析完成，该日志文件中没有时间戳信息或无法使用时间属性进行日志过滤")
                    df_log["line"] = df_log["content"].astype(str)
                elif len(logs) > 0:
                    logger.info("日志解析完成，该日志文件中有时间戳信息，开始进行时间过滤")
                    f_line = df_log.iloc[0].to_dict()
                    f_line_time = f_line.get("timestamp")
                    if pd.isna(step_row["time_format"].values[0]):
                        f_line_time_foramt = self.llm.infer(
                            system_prompt="你是一个时间戳格式生成专家",
                            user_prompt=f"""
                            -**任务**:请你对给定的时间戳进行解析，生成对应的datetime库标准格式
                            -**待解析时间戳**:{f_line_time}
                            -**示例1**: `31/Dec/2024:17:05:41 +0800` 对应标准格式为`%d/%b/%Y:%H:%M:%S %z`
                            -**示例2**: `2024/2/11-12:12:34` 对应标准格式为 `%Y/%m/%d-%M:%S`
                            -**注意**:你的最终输出只有一个时间戳对应的标准格式，不可以包含其他内容
                            """,
                        )
                    else:
                        logger.info("存在时间戳格式，直接进行解析")
                        f_line_time_foramt = step_row["time_format"].values[0]

                    df_log["timestamp"] = pd.to_datetime(
                        df_log["timestamp"], format=f_line_time_foramt, errors="coerce"
                    )
                    start_time = pd.to_datetime(dynamic.get("start_time"))
                    end_time = pd.to_datetime(dynamic.get("end_time"))
                    df_log = df_log[
                        (df_log["timestamp"] >= start_time) & (df_log["timestamp"] <= end_time)
                    ]
                    df_log["line"] = (
                        df_log["timestamp"].astype(str) + " " + df_log["content"].astype(str)
                    )
                else:
                    logger.info("日志解析未发现相关日志！")
                    df_log = pd.DataFrame()

                logger.info(f"经过时间属性过滤后还剩:{len(df_log)}条日志")

                if len(df_log) <= 0:
                    logger.warning("时间过滤后无日志，跳过模板查询")
                    contents = []
                    template_list = []
                    parse_result = []
                else:
                    filtered_log_lines = df_log["content"].astype(str).tolist()
                    file_name = step_row["file_name"].values[0]
                    template_name = f"{file_name}_templates.csv"
                    logger.info(f"使用模板方式查询日志，模板名称: {template_name}")
                    try:
                        matched_logs, template_list, parse_result = (
                            self.di.query_related_log_line_by_log_template_with_result(
                                log_lines=filtered_log_lines,
                                template_name=template_name,
                            )
                        )
                        logger.info(f"模板查询完成，匹配到 {len(matched_logs)} 条相关日志")
                    except Exception as e:
                        logger.error(f"模板查询失败: {e}")
                        matched_logs = []
                        template_list = []
                        parse_result = []

                    if len(matched_logs) > 0:
                        matched_df_log = df_log[df_log["content"].astype(str).isin(matched_logs)]
                        contents = [log for log in matched_df_log["line"]] if len(matched_df_log) > 0 else []
                    else:
                        contents = []

                    logger.info(f"最终用于根因分析的日志条数: {len(contents)}")

            logger.info("根音分析__开始进行阶段性根因分析...")
            self.sf = RootCaseDetector(
                online_log=contents,
                log_name=platform,
                step_row=step_row,
                job={
                    "job": self.job_name,
                    "job_description": row["workchain"].values[0],
                    "check_seq": detect_chain,
                    "pre_root_case": file_analysis_results,
                },
                pre_root_case=file_analysis_results,
                dynamic=dynamic,
                llm_config=self.llm_config,
            )

            stage_root_case = self.sf.analyze()
            logger.info(f"根因分析__当前阶段的根因分析结果为:{stage_root_case}")
            file_analysis_results.append({platform: stage_root_case})

            if len(contents) > 0 and template_list and parse_result:
                matched_logs_for_update = []
                if len(df_log) > 0 and "content" in df_log.columns:
                    matched_df_log = df_log[df_log["line"].isin(contents)]
                    matched_logs_for_update = matched_df_log["content"].astype(str).tolist()

                if matched_logs_for_update:
                    logger.info(
                        f"开始更新动态信息，传入 {len(matched_logs_for_update)} 条日志（与根因分析输入相同）"
                    )
                    self.di.update_from_template_result(
                        matched_logs=matched_logs_for_update,
                        template_list=template_list,
                        parse_result=parse_result,
                    )
                    logger.info("动态信息更新完成")
                else:
                    logger.info("无法找到对应的原始日志行，跳过动态信息更新")
            else:
                logger.info("跳过动态信息更新（无有效日志或缺少模板信息）")

        logger.info(f"根因分析__各文件对应的根因分析结果如下:{file_analysis_results}")
        result = self.sf.summary(
            stage_analysis=file_analysis_results,
            log_info=df_map,
            fault_description=self.fault_description,
            detect_chain_description=detect_chain_description,
        )
        logger.info(f"整个过程提取得到的动态信息为:{self.di.get_content()}")
        logger.info(f"结论____: \n{result}")
        return result, detect_chain
