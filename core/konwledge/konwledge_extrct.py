# coding=utf-8
# @File : konwledge_extrct
# @Project : fault-analysis
# @Description :进行知识库自动化提取
from datetime import datetime
from loguru import logger
import pandas as pd
import os
import ast
import configs
from core.localization.log_tracking_agent import LogTrackingAgent
from core.log_template.log_precondition import time_format_generate
from core.utils import re_extractor, response_extractor
from core.llm.base import LLMClient
from typing import List, Dict, Union

llm_config = configs.LLM_CONFIG
standard_format = "%Y-%m-%d %H:%M:%S"
def log_extract_by_time(path, lib, time_interval):
    """
    将日志文件按照时间进行解析然后根据时间间隔进行切分
    :param path:日志文件路径
    :param lib:日志元数据文件路径
    :param time_interval:时间间隔
    :return:
    """
    # 打开日志文件
    with open(path, 'r', encoding='utf-8')as f:
        logs = f.readlines()
    # 去日志元数据文件中找到对应日志文件元数据中的时间属性
    log_name = os.path.basename(path)
    log_meta_data_lib = pd.read_csv(lib, encoding='utf-8')
    meta_data = log_meta_data_lib[log_meta_data_lib['file_name']==log_name]
    if len(meta_data) == 0 or pd.isna(meta_data['prefix_format'].values[0] or pd.isna(meta_data['time_format']).values[0]):
        logger.warning(f"日志文件{log_name}的元数据记录缺失或不完整")
        # 若记录/时间属性不存在则添加
        # 添加-使用大模型进行日志时间格式生成
        time_regex, datetime_format = time_format_generate(logs)
        # 添加-写回日志元数据文件
        if len(meta_data) == 0:
            log_meta_data_lib.loc[len(log_meta_data_lib)] = {
                "file_name": log_name,
                "prefix_format": time_regex,
                "time_format": datetime_format
            }
        else:
            log_meta_data_lib.loc[meta_data['file_name'] == log_name, 'time_format'] = datetime_format
            log_meta_data_lib.loc[meta_data['file_name'] == log_name, 'prefix_format'] = time_regex

    else:
        # 时间记录存在则继续
        logger.info(f"日志文件{log_name}的元数据记录读取正常")
        time_regex = meta_data[meta_data['file_name'] == log_name]['prefix_format'].iloc[0]
        datetime_format = meta_data[meta_data['file_name'] == log_name]['time_format'].iloc[0]
    start_time = datetime.strptime(time_interval[0], standard_format)
    end_time = datetime.strptime(time_interval[1], standard_format)
    filtered_logs = []
    logger.info(f"按照时间间隔进行日志：{log_name}的日志数据拆分")
    for log in logs:
        time_str = re_extractor(time_regex, log)[0]
        try:
            # 将时间字符串转换为datetime对象
            log_time = datetime.strptime(time_str, datetime_format)
            # 检查时间是否在指定间隔内
            if start_time <= log_time <= end_time:
                filtered_logs.append(log)
        except ValueError:
            logger.warning("时间格式转换失败")
            # 时间格式转换失败的日志处理
            continue
    log_meta_data_lib.to_csv(lib, index=False)
    if len(meta_data) == 0 or pd.isna(meta_data['prefix_format'].values[0] or pd.isna(meta_data['time_format']).values[0]):
        logger.info(f"日志文件{log_name}的元数据记录已补全")
    return filtered_logs




def extract_normal_log_cluster(path, time_interval, lib):
    """
    根据日志内容进行
    :param path:日志文件夹路径
    :param time_interval:时间间隔
    :param lib:日志元数据文件
    :return:
    """
    filtered_logs = {}
    llm = LLMClient(llm_config)
    # 循环读取日志文件
    for file in os.listdir(path):
        # 每个日志文件都按照时间进行分割
        filtered_logs[file] = log_extract_by_time(os.path.join(path, file), lib, time_interval)
    # print(filtered_logs)
    # print(len(filtered_logs))

    # 规定入口日志文件
    input_file = ['https_access.log', 'ssh_access.log']
    log_clusters: List[List[Union[LogTrackingAgent, dict, str]]] = []
    log_meta_data_lib = pd.read_csv(lib, encoding='utf-8')
    for f in input_file:
        meta_data = log_meta_data_lib[log_meta_data_lib['file_name'] == f]
        if pd.dondisna(meta_data['input_log_regex'].values[0]):
        # 使用大模型进行入口日志的关键信息正则表达式生成
            dynamic = response_extractor(
                llm.infer(
                    system_prompt='你是一个日志信息提取专家',
                    user_prompt=PromptLoader.get_prompt(
                        prompt_name='lib/log_key_info_regex_generate.prompt',
                        logs=filtered_logs[f]
                    )
                )
            ).get('result')
            regex = dynamic.get('regex')
            input_dynamic_trace_description = dynamic.get('description')
            # print(regex)
            # print(input_dynamic_trace_description)
        else:
            regex = meta_data['input_log_regex'].values[0]
            input_dynamic_trace_description = ast.literal_eval(meta_data['input_log_description'].values[0])

        for input_log in filtered_logs[f]:
            # 遍历入口日志
            trace_feature = re_extractor(regex, input_log)
            di = LogTrackingAgent()
            di._content.update(dict(zip(trace_feature, input_dynamic_trace_description)))
            log_clusters.append([di, {f: [input_log]}])
    # 日志簇初始化完成
    # print(log_clusters)
    # print(len(log_clusters))
    c = 1
    # 遍历日志簇
    for cluster in log_clusters:
        # 按照关键字进行日志块拆分，生成完整日志簇
        for left_log_file in os.listdir(path):
            if left_log_file not in input_file:
                # 获取按照时间过滤的日志内容
                log = filtered_logs[left_log_file]
                cluster[1][left_log_file] = cluster[0].update(log)
        print(f'簇{c}当前生成的动态信息如下：')
        c +=1
        print(cluster[0].get_content())
    c = 1
    for cluster in log_clusters:
        print('===========================')
        print(f'簇{c}当前筛选得到的日志如下：')
        c += 1
        for f, log in cluster[1].items():
            print(f'日志{f}的过滤结果为: {log}')
        print('===========================')

    # 使用大模型进行日志簇流程判断
    llm = LLMClient(configs.LLM_CONFIG)
    normal_accident_flow = []
    for cluster in log_clusters:
        # 每个日志簇进行一次判断
        response = llm.infer(
            system_prompt='你是一个日志分析专家，你能够根据日志文件分析出日志对应的正常事件流程',
            user_prompt=PromptLoader.get_prompt(
                prompt_name='lib/log_flow_judge.prompt',
                logs=cluster[1]
            )
        )
        print(f'日志内容为：{cluster[1]}')
        print(response)
        result = response_extractor(response).get('result')
        if result.get('right'):
            # 确定是正常流程
            normal_accident_flow.append({
                'log': cluster[1],
                'description': result.get('description'),
                'name': result.get('accident_name')
            })

    # 得到可能的正常日志流程结果并返回
    return normal_accident_flow

def summary_normal_single_process(log_cluster):
    """根据输入可能正常的日志簇进行判断"""
    llm = LLMClient(llm_config)
    result = response_extractor(llm.infer(
        system_prompt='你是一个日志分析专家，你能够根据日志文件分析出日志对应的正常事件流程',
        user_prompt=PromptLoader.get_prompt(
            prompt_name='lib/normal_process_generate_by_log_cluster.prompt',
            logs=log_cluster
        )
    ))
def test():
    result = log_extract_by_time(
        path='resource/log/test.log',
        lib='resource/log_time_format.csv',
        time_interval=['2023-10-01 12:00:00', '2023-10-01 13:00:01']
    )
    print(result)

if __name__ == '__main__':

    os.chdir('E:\Self_code\PycharmProjects\ZTE\\fault-analysis')
    from core.prompts.prompt_loader import PromptLoader
    PromptLoader.from_paths(['core/prompts'])
    log_floder = ''
    time_interval = ['2025-09-09 10:00:00', '2025-09-09 10:00:03']
    print(extract_normal_log_cluster('resource/log', time_interval, 'resource/log_time_format.csv'))

