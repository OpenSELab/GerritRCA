# coding=utf-8
# @Time : 2025/9/8 11:23
# @Author : RoseLee
# @File : konwledge_extrct
# @Project : fault-analysis
# @Description : 进行知识库自动化提取

from loguru import logger
import os
import time
import json
import random
import pandas as pd
from datetime import datetime
from typing import List, Dict, Union, Any
import configs
from core.localization.log_tracking_agent import LogTrackingAgent

# 假设缺失的全局/外部依赖/函数定义（需根据实际环境补全）
class ZteLLMClient:
    def __init__(self, config: dict):
        self.config = config
    def infer(self, system_prompt: str, user_prompt: str) -> dict:
        return {"result": {"right": True, "description": "", "accident_name": "", "chain": ""}}

class PromptLoader:
    @staticmethod
    def get_prompt(prompt_name: str) -> str:
        return ""

class ResponseExtractor:
    def get(self, response: dict) -> dict:
        return response.get("result", {})

response_extractor = ResponseExtractor()
re_extractor = lambda pattern, text: []  # 正则提取函数占位
match_file_name = lambda name, target: True  # 文件名匹配函数占位
time_format_generate = lambda logs, config: (r"\d+-\d+-\d+ \d+:\d+:\d+", "%Y-%m-%d %H:%M:%S")  # 时间格式生成占位
template_generating = lambda **kwargs: ([], [])  # 模板生成函数占位
build_task_workflow_map = lambda flows: {}  # 任务流程构建占位

standard_format = "%Y-%m-%d %H:%M:%S"
_template_cache = {}  # {template_name: (template_list, prefix_format)}

def _get_template_cache_key(template_name: str) -> str:
    """生成模板缓存键"""
    return template_name

def _load_template_if_cached(template_name: str):
    """如果模板已缓存，返回缓存内容；否则返回 None"""
    cache_key = _get_template_cache_key(template_name)
    if cache_key in _template_cache:
        logger.debug(f"从缓存加载模板: {template_name}")
        return _template_cache[cache_key]
    return None

def _cache_template(template_name: str, template_list: List, prefix_format: str = None):
    """缓存模板"""
    cache_key = _get_template_cache_key(template_name)
    _template_cache[cache_key] = (template_list, prefix_format)
    logger.debug(f"缓存模板: {template_name}")

def log_extract_by_time(log_file_path, lib, time_interval, llm_config):
    """
    将日志文件按照时间进行解析然后根据时间间隔进行切分
    :param path:日志文件路径
    :param lib:日志元数据文件路径
    :param time_interval:时间间隔
    :return:
    """
    # 打开日志文件
    with open(log_file_path, 'r', encoding='utf-8') as f:
        logs = f.readlines()
    # 去日志元数据文件中找到对应日志文件元数据中的时间属性
    log_name = os.path.basename(log_file_path)
    log_meta_data_lib = pd.read_csv(lib, encoding='utf-8')

    # 找到对应的元数据行
    meta_mask = log_meta_data_lib['file_name'].apply(match_file_name, args=(log_name,))
    meta_data = log_meta_data_lib[meta_mask]

    need_update_meta = False
    time_regex = None
    datetime_format = None
    # 判断元数据行内容是否存在
    if len(meta_data) == 0 or pd.isna(meta_data['prefix_format'].values[0]) or pd.isna(meta_data['time_format'].values[0]):
        logger.warning(f"日志文件{log_name}的元数据记录缺失或不完整")
        # 若记录/时间属性不存在则添加
        # 添加-使用大模型进行日志时间格式生成
        time_regex, datetime_format = time_format_generate(logs, llm_config)
        # 添加-写回日志元数据文件
        if len(meta_data) == 0:
            # 如果未匹配到说明日志文件的对应记录不存在，因此要新增，在最后新增
            log_meta_data_lib.loc[len(log_meta_data_lib)] = {
                "file_name": log_name,
                "prefix_format": time_regex,
                "time_format": datetime_format
            }
        else:
            ## 存在则直接根据文件名定位到那一行然后新增对应值
            log_meta_data_lib.loc[meta_data['file_name'].apply(match_file_name, args=(log_name,)), 'time_format'] = datetime_format
            log_meta_data_lib.loc[meta_data['file_name'].apply(match_file_name, args=(log_name,)), 'prefix_format'] = time_regex
    else:
        # 时间的正则表达式和标准datetime格式存在则继续
        logger.info(f"日志文件{log_name}的元数据记录读取正常")
    # 读取当前行的正则表达式和标准datetime格式
    time_regex = meta_data[meta_data['file_name'].apply(match_file_name, args=(log_name,))]['prefix_format'].iloc[0]
    datetime_format = meta_data[meta_data['file_name'].apply(match_file_name, args=(log_name,))]['time_format'].iloc[0]
    # 先按照标准格式对开始和结束时间进行解析
    start_time = datetime.strptime(time_interval[0], standard_format)
    end_time = datetime.strptime(time_interval[1], standard_format)
    logger.info(f"按照时间间隔 [{start_time}, {end_time}] 进行日志 {log_name} 的数据拆分。")

    filtered_logs = []
    processed_count = 0
    matched_count = 0

    # 遍历日志
    with open(log_file_path, 'r', encoding='utf-8') as f:
        for line in f:
            processed_count += 1
            # 为了避免内存爆炸，可以定期打印进度
            if processed_count % 1000000 == 0:
                logger.info(f"已处理 {processed_count} 行日志...")

            match_result = re_extractor(time_regex, line)
            if not match_result:
                continue

            try:
                time_str = ' '.join(match_result[:2])
            except (IndexError, TypeError):
                continue

            try:
                log_time = datetime.strptime(time_str, datetime_format)
                if start_time <= log_time <= end_time:
                    filtered_logs.append(line)
                    matched_count += 1
            except ValueError:
                continue

    logger.info(f"处理完成！总共处理 {processed_count} 行，筛选出 {matched_count} 行。")
    if need_update_meta:
        log_meta_data_lib.to_csv(lib, index=False, encoding='utf-8')
    if len(meta_data) == 0 or pd.isna(meta_data['prefix_format'].values[0]) or pd.isna(meta_data['time_format'].values[0]):
        logger.info(f"日志文件{log_name}的元数据记录已补全")
    return filtered_logs

def extract_normal_log_cluster_single_time_windows(path, time_interval, lib, llm_config, input_file):
    """
    根据单时间间隔中的日志内容进行正常跨系统任务流程挖掘
    :param path:日志文件夹路径
    :param time_interval:时间间隔
    :param lib:日志元数据文件
    :return:
    """
    filtered_logs = {}
    llm = ZteLLMClient(llm_config)
    start = time.time()
    # 循环读取日志文件
    for file in os.listdir(path):
        # 每个日志文件都按照时间进行分割
        filtered_logs[file] = log_extract_by_time(os.path.join(path, file), lib, time_interval, llm_config)
        logger.info("=====================================")
        logger.info(file)
        logger.info(len(filtered_logs[file]))
        logger.info("=====================================")
    logger.info(f"日志文件过滤完成，耗时：{time.time() - start:.2f}秒")

    # 规定入口日志文件
    log_clusters: List[List[Union[LogTrackingAgent, dict, str]]] = []
    log_meta_data_lib = pd.read_csv(lib, encoding='utf-8')

    logger.info("开始批量处理入口日志...")
    for f in input_file:
        # 生成模板名
        log_name_no_ext = os.path.splitext(f)[0]
        template_name = f"{log_name_no_ext}_templates.csv"

        # 获取该入口文件的所有日志
        input_logs = filtered_logs.get(f, [])
        if not input_logs:
            logger.warning(f"入口日志文件 {f} 没有日志数据，跳过")
            continue

        logger.info(f"处理入口日志文件 {f}，共 {len(input_logs)} 条日志")

        # 批量调用 template_generating，一次性处理所有日志
        try:
            logger.info(f"批量调用 template_generating 处理 {len(input_logs)} 条日志...")
            template_list, parse_result = template_generating(
                llm_config=llm_config,
                log_file_line=input_logs,  # 批量处理所有日志
                template_name=template_name
            )
            logger.info(f"批量处理完成，生成 {len(template_list)} 个模板，解析 {len(parse_result)} 条日志")
        except Exception as e:
            logger.error(f"批量处理入口日志文件 {f} 时出错: {e}")
            import traceback
            traceback.print_exc()
            continue

        # 构建模板到占位符解释的映射
        template_placeholder_map = {}
        for template_row in template_list:
            if len(template_row) > 0:
                template = template_row[0]
                placeholders = template_row[1:] if len(template_row) > 1 else []
                template_placeholder_map[template] = placeholders

        for parse_item in parse_result:
            if len(parse_item) < 2:
                continue
            original_log = parse_item[0]
            template = parse_item[1]
            parameters = parse_item[2:] if len(parse_item) > 2 else []
            placeholders = template_placeholder_map.get(template, [])

            di = LogTrackingAgent()
            # 将参数和占位符解释按照 kv 对放入动态信息管理器 (排除占位符解释为 "-1" 的)
            for idx, param_value in enumerate(parameters):
                if idx < len(placeholders):
                    placeholder_desc = placeholders[idx]
                    if placeholder_desc == "-1":
                        continue
                    param_value_str = str(param_value)
                    if param_value_str not in di._content:
                        di._content[param_value_str] = placeholder_desc

            logger.debug(f"从入口日志 {str(original_log)[:50]}... 中提取的动态信息: {di._content}")
            log_clusters.append([di, {f: [original_log]}])

    logger.info(f"日志簇初始化完成，共 {len(log_clusters)} 个日志簇")

    # ==================================================
    # 第二阶段: 补全日志簇
    # ==================================================
    logger.info("开始补全日志簇，先对非入口日志进行一次性解析并缓存结果...")

    # 缓存结构: {left_log_file: (template_list, parse_result)}
    parsed_log_cache: Dict[str, tuple] = {}

    for left_log_file in os.listdir(path):
        if left_log_file in input_file:
            continue
        # 获取按照时间过滤的日志内容
        log_lines = filtered_logs.get(left_log_file, [])
        if not log_lines:
            logger.warning(f"非入口日志文件 {left_log_file} 在当前时间窗口内没有日志，跳过解析")
            continue

        try:
            # 生成模板名
            log_name_no_ext = os.path.splitext(left_log_file)[0]
            template_name = f"{log_name_no_ext}_templates.csv"

            logger.info(f"为非入口日志文件 {left_log_file} 进行一次性模板解析，模板名: {template_name}")
            template_list, parse_result = template_generating(
                llm_config=llm_config,
                log_file_line=log_lines,
                template_name=template_name
            )
            parsed_log_cache[left_log_file] = (template_list, parse_result)
            logger.info(f"日志文件 {left_log_file} 解析完成，模板数: {len(template_list)}, 解析行数: {len(parse_result)}")
        except Exception as e:
            logger.error(f"解析非入口日志文件 {left_log_file} 时出错: {e}")
            import traceback
            traceback.print_exc()
            # 出错时，不缓存该文件的解析结果
            continue

    c = 1
    start = time.time()
    # 遍历日志簇，使用缓存的解析结果进行补全
    for cluster in log_clusters:
        for left_log_file in os.listdir(path):
            if left_log_file in input_file:
                continue

            # 如果该文件没有解析结果缓存，跳过
            if left_log_file not in parsed_log_cache:
                logger.warning(f"日志文件 {left_log_file} 没有可用的解析缓存，跳过该文件的补全")
                cluster[1][left_log_file] = []
                continue

            template_list, parse_result = parsed_log_cache[left_log_file]
            try:
                # 使用已解析结果进行日志定位，避免重复解析
                matched_logs = cluster[0].query_related_log_line_by_parsed_result(
                    template_list=template_list,
                    parse_result=parse_result
                )
                cluster[1][left_log_file] = matched_logs
            except Exception as e:
                logger.error(f"使用缓存解析结果更新日志簇时出错（文件: {left_log_file}）: {e}")
                import traceback
                traceback.print_exc()
                cluster[1][left_log_file] = []

        logger.info(f'簇{c}当前生成的动态信息如下: {cluster[0].get_content()}')
        c += 1

    # 过滤空日志簇
    log_clusters_no_empty = [cluster for cluster in log_clusters if sum(len(sub_arr) for sub_arr in cluster[1].values()) > 1]

    # 打印筛选结果
    for cluster in log_clusters_no_empty:
        logger.info('=====================================')
        logger.info(f'簇{c}当前筛选得到的日志如下: ')
        c += 1
        for f, log in cluster[1].items():
            logger.info(f'日志{f}的过滤结果为: {len(log)} 条日志')
        logger.info('=====================================')

    logger.info(f'补全日志簇花费时间: {time.time() - start:.2f}秒')

    # 保存结果到JSON
    with open(f'resource/lib/case/log_cluster/{time_interval[0]}--{time_interval[1]}/cluster_result.json', 'w', encoding='utf-8') as f:
        json.dump(log_clusters_no_empty, f, ensure_ascii=False, indent=4)

    # 生成正常流程
    normal_accident_flow = single_process_generate_by_log_cluster(llm_config, log_clusters_no_empty)

    # 归一化并返回
    if not normal_accident_flow:
        logger.warning("没有找到有效的正常流程，返回空列表")
        return []

    logger.info(f"开始对 {len(normal_accident_flow)} 个日志簇进行任务聚类和流程归一化")
    workflow_map = build_task_workflow_map(normal_accident_flow)
    logger.info(f"任务流程归一化完成，共生成 {len(workflow_map)} 个任务流程")

    return workflow_map

import json
import time
import random
import os
import logging
from datetime import datetime

logger = logging.getLogger(__name__)
standard_format = '%Y-%m-%d %H:%M:%S'

def single_process_generate_by_log_cluster(llm_config, log_clusters):
    """
    根据输入日志簇生成对应的单次流程描述
    :param llm_config: 大模型配置
    :param log_clusters: 日志簇 List[List[Dy,Dict[str, List]]]
    :return:
    """
    # 使用大模型进行日志簇流程判断
    llm = ZteLLMClient(llm_config)
    normal_accident_flow = {}
    fail_accident_flow = []
    vaild_count = 0

    with open('resource/lib/case/tmp/normal_accident_flow_tmp.json', 'a') as f:
        f.write("[")
    with open('resource/lib/case/tmp/fail_accident_flow_tmp.json', 'a') as f:
        f.write("[")

    for cluster in log_clusters:
        flag = True
        while flag:
            # 每个日志簇进行一次判断
            try:
                response = llm.infer(
                    system_prompt='你是一个日志分析专家，你能够根据日志文件分析出日志对应的正常事件流程',
                    user_prompt=PromptLoader.get_prompt(
                        prompt_name='lib/log_flow_judge.prompt',
                        logs=cluster[1]
                    )
                )
                flag = False
            except Exception as e:
                print(cluster[1])
                logger.warning(f"模型请求失败，等待20s重新请求")
                logger.info("等待中...")
                time.sleep(20)

        result = response_extractor(response).get('result')
        if result.get('right'):
            # 确定是正常流程
            normal_accident_flow[vaild_count] = ({
                'log': cluster[1],
                'description': result.get('description'),
                'name': result.get('accident_name'),
                'chain': result.get('chain')
            })
            logger.info(f'簇{vaild_count}解析完成，是一个完整的正常流程')
            with open('resource/lib/case/tmp/normal_accident_flow_tmp.json', 'a') as f:
                if vaild_count > 0:
                    f.write(",")
                f.write(json.dumps(normal_accident_flow[vaild_count], ensure_ascii=False, indent=4))
        else:
            failed_cluster = {
                'log': cluster[1],
                'description': result.get('description'),
                'name': result.get('accident_name'),
                'chain': result.get('chain')
            }
            fail_accident_flow.append(failed_cluster)
            logger.warning(f'簇{vaild_count}解析完成，流程非正常，原因如下：')
            logger.info(result.get('description'))
            with open('resource/lib/case/tmp/fail_accident_flow_tmp.json', 'a') as f:
                if vaild_count > 0:
                    f.write(",")
                f.write(json.dumps(failed_cluster, ensure_ascii=False, indent=4))
        # 如果需要进行流程生成则+1
        vaild_count += 1
    logger.info(f'有效流程数为: {vaild_count}')

    # 得到可能的正常日志流程结果并返回
    with open('resource/lib/case/time_window_normal_accident_flow.json', 'w') as f:
        json.dump(normal_accident_flow, f, ensure_ascii=False, indent=4)
    with open('resource/lib/case/time_window_fail_accident_flow.json', 'w') as f:
        json.dump(fail_accident_flow, f, ensure_ascii=False, indent=4)

    return normal_accident_flow

# 全局模板缓存，用于在多个时间窗口之间共享模板
_template_cache = {}  # {template_name: (template_list, prefix_format)}

def _get_template_cache_key(template_name: str) -> str:
    """生成模板缓存键"""
    return template_name

def _load_template_if_cached(template_name: str):
    """如果模板已缓存，返回缓存内容；否则返回 None"""
    cache_key = _get_template_cache_key(template_name)
    if cache_key in _template_cache:
        logger.debug(f"从缓存加载模板: {template_name}")
        return _template_cache[cache_key]
    return None

def _cache_template(template_name: str, template_list: list, prefix_format: str = None):
    """缓存模板"""
    cache_key = _get_template_cache_key(template_name)
    _template_cache[cache_key] = (template_list, prefix_format)
    logger.debug(f"缓存模板: {template_name}")

def extract_normal_log_cluster(start_time: str, end_time: str, path: str, lib: str, sample_points: int,
                               llm_config: dict, input_file: list[str]):
    """
    根据开始时间、结束时间和采样点数量，在多个时间窗口中进行正常跨系统任务流程挖掘
    :param start_time: 开始时间，格式为 'YYYY-MM-DD HH:MM:SS'
    :param end_time: 结束时间，格式为 'YYYY-MM-DD HH:MM:SS'
    :param path: 日志文件夹路径
    :param lib: 日志元数据文件路径
    :param sample_points: 采样点数量（每个采样点对应一个3秒的时间窗口）
    :param llm_config: 大模型配置
    :param input_file: 入口日志文件列表
    :return: List[Dict[str, str]], 格式为 [{name: "任务名", description: "任务描述", chain: "日志链"}, ...]
    """
    logger.info("=" * 60)
    logger.info("开始多采样点正常跨系统任务流程挖掘")
    logger.info(f"开始时间: {start_time}")
    logger.info(f"结束时间: {end_time}")
    logger.info(f"采样点数量: {sample_points}")
    logger.info("=" * 60)

    # 清空模板缓存，确保使用最新的模板
    global _template_cache
    _template_cache.clear()
    logger.info("已清空模板缓存，准备开始新的提取流程")

    # 解析时间
    start_dt = datetime.strptime(start_time, standard_format)
    end_dt = datetime.strptime(end_time, standard_format)
    total_seconds = (end_dt - start_dt).total_seconds()

    # 检查时间区间是否足够
    window_seconds = 3  # 每个时间窗口为3秒
    required_seconds = sample_points * window_seconds
    if total_seconds < required_seconds:
        error_msg = f"时间区间不足: 总时长 {total_seconds} 秒，需要至少 {required_seconds} 秒 ({sample_points} 个采样点 × 3秒)"
        logger.error(error_msg)
        raise ValueError(error_msg)

    # 生成随机不重复的时间窗口
    max_start_time = end_dt.timestamp() - window_seconds
    min_start_time = start_dt.timestamp()
    available_range = max_start_time - min_start_time

    # 生成随机时间窗口的开始时间点（不重复）
    time_windows = []
    attempts = 0
    max_attempts = sample_points * 10  # 最多尝试次数

    while len(time_windows) < sample_points and attempts < max_attempts:
        attempts += 1
        # 随机选择一个开始时间点
        random_start = min_start_time + random.uniform(0, available_range)
        window_start = datetime.fromtimestamp(random_start)
        window_end = datetime.fromtimestamp(random_start + window_seconds)

        # 检查是否与已有窗口重叠
        overlap = False
        for existing_start, existing_end in time_windows:
            if not (window_end <= existing_start or window_start >= existing_end):
                overlap = True
                break
        if not overlap:
            time_windows.append((window_start, window_end))
            logger.info(
                f"生成时间窗口 {len(time_windows)}: {window_start.strftime(standard_format)} - "
                f"{window_end.strftime(standard_format)}"
            )

    if len(time_windows) < sample_points:
        logger.warning(f"无法生成足够的非重叠时间窗口，实际生成 {len(time_windows)} 个，需要 {sample_points} 个")

    # 对每个时间窗口调用 extract_normal_log_cluster_single_time_windows
    all_workflow_results = []
    for idx, (window_start, window_end) in enumerate(time_windows, 1):
        logger.info(
            f"\n处理时间窗口 {idx}/{len(time_windows)}: {window_start.strftime(standard_format)} - "
            f"{window_end.strftime(standard_format)}"
        )
        time_interval = [window_start.strftime(standard_format), window_end.strftime(standard_format)]

        try:
            workflow_result = extract_normal_log_cluster_single_time_windows(
                path=path,
                time_interval=time_interval,
                lib=lib,
                llm_config=llm_config,
                input_file=input_file
            )
            if workflow_result:
                all_workflow_results.extend(workflow_result)
                logger.info(f"时间窗口 {idx} 完成，获得 {len(workflow_result)} 个任务流程")
            else:
                logger.warning(f"时间窗口 {idx} 未获得任何任务流程")
        except Exception as e:
            logger.error(f"处理时间窗口 {idx} 时出错: {e}")
            import traceback
            traceback.print_exc()
            continue

    if not all_workflow_results:
        logger.warning("所有时间窗口都未获得有效的任务流程，返回空列表")
        return []

    logger.info(f"\n所有时间窗口处理完成，共获得 {len(all_workflow_results)} 个任务流程结果")
    logger.info("开始对多个时间窗口的结果进行归一化...")

    # 将结果转换为编号字典格式，用于 build_task_workflow_map
    # all_workflow_results 是 List[Dict[str, str]], 格式为 [{name, description, chain}, ...]
    # build_task_workflow_map 期望 Dict[int, Dict[str, Any]], 格式为 {编号: {log, description, name, chain}}
    # 由于 all_workflow_results 已经是归一化后的结果，我们需要将其转换为期望的格式
    numbered_workflows = {}
    for idx, workflow in enumerate(all_workflow_results):
        # 将 workflow 转换为 build_task_workflow_map 期望的格式
        numbered_workflows[idx] = {
            'log': {},  # 归一化后的结果中没有原始日志，使用空字典
            'description': workflow.get('description', ''),
            'name': workflow.get('name', ''),
            'chain': workflow.get('chain', '')
        }

    # 使用 build_task_workflow_map 进行归一化
    normalized_workflows = build_task_workflow_map(numbered_workflows)

    logger.info(f"归一化完成，共生成 {len(normalized_workflows)} 个归一化任务流程")
    logger.info("=" * 60)

    with open('resource/lib/case/final_normalized_workflows.json', 'w') as f:
        json.dump(normalized_workflows, f, ensure_ascii=False, indent=4)
    return normalized_workflows

if __name__ == '__main__':
    os.chdir('/home/6000015813/PycharmProjects/fault-analysis')
    from core.prompts.prompt_loader import PromptLoader
    PromptLoader.from_paths(['core/prompts'])
    log_floder = '/home/6000015813/log_to_analysis/now'
    time_interval = ['2025-07-29 09:56:42', '2025-07-29 09:56:44']
    input_file = ['https_access.log', 'ssh_access.log']

    extract_normal_log_cluster(
        start_time='2025-07-29 09:50:00',
        end_time='2025-07-29 09:59:59',
        path='/home/6000015813/log_to_analysis/now',
        lib='resource/code_map.csv',
        sample_points=10,
        llm_config=configs.ZTE_V3_CONFIG,
        input_file=['https_access.log', 'ssh_access.log']
    )