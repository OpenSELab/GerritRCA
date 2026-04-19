# coding=utf-8
# @File : add_time
# @Project : fault-analysis
# @Description :为已经有的json添加时间

import json
import os
import time

from core.utils import response_extractor
import configs
from core.llm.base import LLMClient

def modify_json_file(file_path):
    # 1. 检查文件是否存在
    if not os.path.exists(file_path):
        print(f"错误：文件 {file_path} 不存在！")
        return

    try:
        # 2. 读取JSON文件
        with open(file_path, 'r', encoding='utf-8') as f:
            # 将JSON字符串解析为Python字典/列表
            data = json.load(f)

        # 3. 修改JSON数据（核心逻辑，根据你的需求调整）
        print("修改前的JSON数据：")
        print(json.dumps(data, indent=4, ensure_ascii=False))

        # 示例修改：根据数据类型（字典/列表）进行修改
        if isinstance(data, dict):
            llm = LLMClient(configs.LLM_CONFIG)
            while True:
                time.sleep(5)
                try:
                    result = response_extractor(llm.infer(
                        system_prompt='你是一个案例分析专家',
                        user_prompt=f'''
                        对于以下案例（存放在json体中）：
                        {data}
                        其中给出了故障描述信息和所涉及的日志，你需要根据这些内容找事件的最早时间和最晚时间，给出他们的标准datetime格式，这两个属性的作用是帮助我缩小排查范围，因此需要你给出开始和结束事件
                        开始时间要求：最早时间-5分钟
                        结束时间要求：最晚时间+5分钟
                        开始和结束时间需要以%Y-%m-%d %H:%M:%S的datetime格式给出，最后放在以下json体给出
                        ## 输出示例
                        ```json
                        {{
                            "start_time":"开始时间",
                            "end_time":"结束时间"
                        }}
                        ```
                        '''
                    ))
                    start_time = result.get("start_time")
                    end_time = result.get("end_time")
                    data['start_time'] = start_time
                    data['end_time'] = end_time
                    break
                except:
                    continue

        elif isinstance(data, list):
            data.append({'name': '新增元素'})
            if len(data) > 0:
                data[0] = '修改第一个元素'  # 修改第一个元素

        # 4. 将修改后的数据写回JSON文件
        with open(file_path, 'w', encoding='utf-8') as f:
            # indent=4 让JSON格式化输出，更易读；ensure_ascii=False 支持中文
            json.dump(data, f, indent=4, ensure_ascii=False)

        print("\n修改后的JSON数据已写回文件：")
        print(json.dumps(data, indent=4, ensure_ascii=False))

    except json.JSONDecodeError:
        print(f"错误：文件 {file_path} 不是有效的JSON格式！")
    except PermissionError:
        print(f"错误：没有权限读写文件 {file_path}！")
    except Exception as e:
        print(f"未知错误：{str(e)}")


def get_all_json_files_recursive(root_folder):
    json_file_paths = []

    # 1. 检查根文件夹是否有效
    if not os.path.isdir(root_folder):
        print(f"错误：文件夹 {root_folder} 不存在或不是有效文件夹！")
        return json_file_paths

    # 2. 递归遍历所有子文件夹和文件
    for root, dirs, files in os.walk(root_folder):
        # 遍历当前文件夹下的所有文件
        for file_name in files:
            # 筛选后缀为 .json 的文件（不区分大小写）
            if file_name.lower().endswith('.json'):
                # 拼接完整文件路径
                full_file_path = os.path.join(root, file_name)
                json_file_paths.append(full_file_path)

    return json_file_paths

# 示例调用
if __name__ == "__main__":

    json_floder = ""
    json_file_paths = get_all_json_files_recursive(json_floder)
    for i in json_file_paths:
        modify_json_file(i)
