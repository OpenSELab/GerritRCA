# coding=utf-8
# @Time : 2026/3/19 17:26
# @Author : RoseLee
# @File : run
# @Project : zte-fault-analysis
# @Description :
import json
import os
import time

import pandas as pd

from core.prompts.prompt_loader import PromptLoader
from core.utils import get_all_json_files_recursive
from loguru import logger
from core.RCA import WorkFlowDivider
import configs


def main(path, result_path='core/result/result.json'):
    # 读取案例json
    cases = get_all_json_files_recursive(path)
    #
    PromptLoader.from_paths(['core/prompts'])
    #循环读入
    for case in cases:
        start = time.time()
        # 在以下两种情况下不进行案例分析
        # 1.日志不全
        if case.endswith('_n.json'):
            continue
        with open(case, 'r', encoding='utf-8') as f:
            case_data = json.load(f)
        # 2.案例本身没答案
        if case_data.get("result") == '-1':
            continue

        #获取当前案例的所属关系
        case_path, case_name = os.path.split(case)
        _, case_time = os.path.split(case_path)

        # 3.已经完成过了
        with open(result_path, 'r', encoding='utf-8') as f:
            # 将JSON字符串解析为Python字典/列表
            data_list = json.load(f)
        for i in data_list:
            if i.get("case_time") == case_time and i.get("case_name") == case_name:
                logger.info(f"当前案例: {case}已经验证过了")
                continue

        count = 1
        while True:
            # 先读取已有的json内容
            with open(result_path, 'r', encoding='utf-8') as f:
                # 将JSON字符串解析为Python字典/列表
                data_list = json.load(f)

            logger.add(f'core/logs/exp1/{case_name}_根因分析实验日志.log',
                       enqueue=True)
            df = pd.read_csv('resource/ci_related_workflow.csv')

            job_name = "gerrit代码问题"
            row = df[df['job'] == job_name]
            jd = WorkFlowDivider(
                job_name=job_name,
                fault_description=case_data.get('description'),
                llm_config=configs.ZTE_V3_CONFIG)

            try:
                logger.info(f"开始为案例: {case}进行故障根因定位")
                result, detect_chain = jd.build_detect_chain(
                    file_description_map_path='resource/code_map.csv',
                    row=row,
                    dynamic={
                        'start_time': case_data.get('start_time'),
                        'end_time': case_data.get('end_time'),
                        'description': case_data.get('description')
                    })
                logger.success("执行成功")
                break
            except Exception as e:
                if isinstance(e, KeyboardInterrupt):
                    pass
                else:
                    logger.error("执行出错, 重新执行")
                    count += 1

        end = time.time()
        cost = end-start
        logger.info(f"共花费{cost}s,共执行{count}轮")
        process_result = {
            "human_result": case_data.get("result"),
            "method_result": result,
            "case_name": case_name,
            "case_time": case_time,
            "cost": cost,
            "detect_chain": detect_chain,
            "round": count
        }
        data_list.append(process_result)
        # 如果正常执行返回结果了，则保存记录直接退出
        with open(result_path, 'w') as f:
            json.dump(data_list, f, indent=4, ensure_ascii=False)


if __name__ == '__main__':
    main('/home/6000015813/PycharmProjects/dataset-create/c/cases')