# coding=utf-8
# @Time : 2025/12/09
# @Author : RoseLee
# @File : log_cluster_workflow
# @Project : fault-analysis
# @Description :
"""
基于正常日志簇，利用大模型完成任务聚类与流程归一化。
"""
from typing import Dict, List, Any

import configs
from core.llm.zte_llm_v3 import ZteLLMClient
from core.utils import response_extractor
from core.prompts.prompt_loader import PromptLoader
from loguru import logger
import json
import os


class LogClusterWorkflowBuilder:
    """
    将正常日志簇聚类为专业任务名，并生成归一化的执行流程描述。
    步骤:
        1. 使用大模型根据 name 聚同一事件 -> {任务名: [编号]}
        2. 针对每个任务，将若干 description 和 chain 交给大模型生成统一流程描述
        3. 返回 [{name: "任务名", description: "任务描述", chain: "日志链"}, ...]

    注意: 输入的 clusters 应为 {编号: 日志簇} 字典格式
    """

    def __init__(self, llm_config: Dict[str, Any] = configs.ZTE_V3_CONFIG, max_desc_per_task: int = 5):
        self.llm = ZteLLMClient(llm_config=llm_config)
        self.max_desc_per_task = max_desc_per_task
        # 初始化 PromptLoader
        PromptLoader.from_paths(['core/prompts'])

    def _build_group_prompt(self, numbered_clusters: Dict[int, Dict[str, Any]]) -> str:
        """
        构造聚类 prompt，使用 PromptLoader 加载模板
        """
        numbered_items = [(idx, cluster.get('name', '')) for idx, cluster in numbered_clusters.items()]
        prompt = PromptLoader.get_prompt(
            "lib/task_cluster.prompt",
            numbered_items=numbered_items
        )
        return prompt

    def group_by_task(self, numbered_clusters: Dict[int, Dict[str, Any]]) -> Dict[str, List[int]]:
        """
        调用大模型，根据 name 聚同一事件，返回 {任务名: [编号]}
        """
        logger.info(f"开始进行任务聚类，共 {len(numbered_clusters)} 个日志簇")
        prompt = self._build_group_prompt(numbered_clusters)
        response = self.llm.infer(
            system_prompt="你是跨系统日志任务的聚类专家，擅长用简洁专业的任务名归并同类事件。",
            user_prompt=prompt
        )
        task_group = response_extractor(response)
        logger.info(f"任务聚类完成，共识别出 {len(task_group)} 个任务")
        return task_group

    def normalize_descriptions(
        self,
        numbered_clusters: Dict[int, Dict[str, Any]],
        task_group: Dict[str, List[int]],
    ) -> List[Dict[str, str]]:
        """
        针对每个任务，将若干 description 和 chain 交给大模型生成统一流程描述
        返回 [{name: "任务名", description: "任务描述", chain: "日志链"}, ...]
        """
        logger.info(f"开始生成任务流程描述，共 {len(task_group)} 个任务")
        result_list = []
        for idx, (task_name, ids) in enumerate(task_group.items(), 1):
            logger.info(f"正在处理任务 {idx}/{len(task_group)}: {task_name}")
            # 收集所有 description 和 chain，用于综合总结（单个描述可能不全）
            descriptions = []
            chains = []
            for i in ids:
                cluster = numbered_clusters.get(i, {})
                desc = cluster.get("description")
                chain = cluster.get("chain", "")
                if desc:
                    descriptions.append(desc)
                if chain:
                    chains.append(chain)
            # 如果描述太多，限制数量防止 prompt 过长
            if len(descriptions) > self.max_desc_per_task:
                logger.info(
                    f"任务 {task_name} 有 {len(descriptions)} 个描述，选取前 {self.max_desc_per_task} 个进行总结"
                )
                descriptions = descriptions[:self.max_desc_per_task]
                chains = chains[:self.max_desc_per_task]

            if not descriptions:
                logger.warning(f"任务 {task_name} 没有可用的描述信息，跳过")
                continue

            # 合并 chain，如果多个 chain 相同则只保留一个，否则选择第一个作为代表
            unique_chains = list(dict.fromkeys(chains))  # 保持顺序去重
            representative_chain = unique_chains[0] if unique_chains else ""

            response = self.llm.infer(
                system_prompt="你是跨系统任务流程的标准化专家，负责生成包含完整执行路径和日志记录的详细任务描述。",
                user_prompt=PromptLoader.get_prompt(
                    "lib/task_description_normalize.prompt",
                    task_name=task_name,
                    descriptions=descriptions,
                    chains=chains
                )
            )
            result = response_extractor(response)
            description = result.get("description", "")

            result_list.append({
                "name": task_name,
                "description": description,
                "chain": representative_chain
            })
            logger.info(f"任务 {task_name} 流程描述生成完成")
        logger.info(f"所有任务流程描述生成完成，共生成 {len(result_list)} 个任务描述")
        return result_list

    def build_workflow_map(self, numbered_clusters: Dict[int, Dict[str, Any]]) -> List[Dict[str, str]]:
        """
        综合两步得到最终结果: [{name: "任务名", description: "任务描述", chain: "日志链"}, ...]

        Args:
            numbered_clusters: {编号: 日志簇} 字典格式

        Returns:
            任务流程列表，每个元素包含 name、description 和 chain
        """
        logger.info("开始构建工作流映射")
        task_group = self.group_by_task(numbered_clusters)
        result = self.normalize_descriptions(numbered_clusters, task_group)
        logger.info("工作流映射构建完成")
        return result


def build_task_workflow_map(numbered_clusters: Dict[int, Dict[str, Any]]) -> List[Dict[str, str]]:
    """
    便捷函数: 输入编号后的日志簇字典 {编号: 日志簇}，输出任务流程列表

    Args:
        numbered_clusters: {编号: 日志簇} 字典格式，编号应为整数

    Returns:
        [{name: "任务名", description: "任务描述", chain: "日志链"}, ...]
    """
    builder = LogClusterWorkflowBuilder()
    return builder.build_workflow_map(numbered_clusters)


def load_clusters_from_file(path: str) -> Dict[int, Dict[str, Any]]:
    """
    从文件加载日志簇数据。文件应为 JSON 字典格式: {编号: 日志簇}

    Args:
        path: JSON 文件路径

    Returns:
        {编号: 日志簇} 字典，编号会被转换为整数
    """
    logger.info(f"开始加载日志簇文件: {path}")
    if not os.path.isfile(path):
        logger.error(f"未找到文件: {path}")
        raise FileNotFoundError(f"未找到文件: {path}")
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    try:
        data = json.loads(content)
    except json.JSONDecodeError as e:
        logger.error(f"文件内容不是有效的 JSON: {e}")
        raise ValueError(f"文件内容不是有效的 JSON: {e}")
    if not isinstance(data, dict):
        logger.error("文件内容应为字典格式: {编号: 日志簇}")
        raise ValueError("文件内容应为字典格式: {编号: 日志簇}")
    # 将字符串键转换为整数键
    numbered_clusters = {}
    for key, value in data.items():
        try:
            num_key = int(key)
            numbered_clusters[num_key] = value
        except (ValueError, TypeError):
            logger.error(f"字典的键应为整数编号，但遇到: {key}")
            raise ValueError(f"字典的键应为整数编号，但遇到: {key}")
    logger.info(f"日志簇文件加载完成，共加载 {len(numbered_clusters)} 个日志簇")
    return numbered_clusters


__all__ = [
    "LogClusterWorkflowBuilder",
    "build_task_workflow_map",
    "load_clusters_from_file",
]

if __name__ == "__main__":
    """
    示例: 从文件加载日志簇并调用 build_task_workflow_map
    文件需存储字典格式的日志簇，如:
    {
        "1": {
            "log": {"gerrit.log": ["..."]},
            "description": "用户于2025-07-29 09:56:44从IP 10.90.241.184通过浏览器发起GET请求查看变更23071864的详情页面，请求首先经过https_access...",
            "name": "用户查看变更评审详情",
            "chain": "https_access.log;httpd_log;gerrit-stderr---supervisor-ssz_wi.log.15"
        },
        "2": {
            "log": {"gerrit.log": ["..."]},
            "description": "从 gerrit 下载代码变更",
            "name": "代码下载",
            "chain": "gerrit.log;ssh_access.log"
        }
    }
    注意: chain 字段为字符串格式，用分号 (;) 分隔日志文件名，表示日志记录的日志顺序
    """
    logger.info("=" * 50)
    logger.info("开始执行日志簇工作流构建任务")
    logger.info("=" * 50)

    clusters_path = "resource/lib/case/time_window_normal_accident_flow.json"
    numbered_clusters = load_clusters_from_file(clusters_path)

    logger.info("开始构建任务工作流映射")
    result = build_task_workflow_map(numbered_clusters)

    logger.info("=" * 50)
    logger.info("任务归一化结果: ")
    logger.info("=" * 50)
    for item in result:
        logger.info(f"任务名: {item['name']}")
        logger.info(f"描述: {item['description']}")
        logger.info(f"日志链: {item['chain']}")
        logger.info("-" * 50)

    logger.info(f"共生成 {len(result)} 个任务流程描述")
    logger.info("=" * 50)