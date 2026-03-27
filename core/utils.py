# coding=utf-8
# @Time : 2025/7/23 9:55
# @Author : RoseLee
# @File : utils
# @Project : fault-analysis
# @Description :

import json
import re
import subprocess
from typing import List


def response_extractor(response):
    """
    提取大模型返回的json格式的结果
    :param response: 大模型返回的json格式的回复
    :return:
    """
    regex = r'```json\n(.*?)\n```'
    json_s = re.search(regex, response, re.DOTALL).group(1)
    # print(json_s)
    result = json.loads(json_s)
    return result

def query_by_statement(statement):
    """
    执行传入的linux查询语句
    :param statement:
    :return:
    """
    result = subprocess.run(
        statement,
        shell=True,
        capture_output=True,
        text=True
    )
    return result.stdout.splitlines()

def re_extractor(regex, content):
    """
    根据传入内容和正则表达式进行匹配并返回结果
    :param regex:
    :param content:
    :return:
    """
    a = re.compile(regex)
    match = a.search(content)
    if match:
        return match.groups()

def match_file_name(source, target):
    """
    判断当前文件名包含于哪个日志描述文件记录行
    :param source: 日志描述文件中的文件名
    :param target: 当前的文件名
    :return: 判断True or False
    """
    match = re.match(r'(.+?)(?:\.[^.]*$|$)', source)
    pure_name = match.group(1) if match else source
    if pure_name in target:
        return True
    return False
def preprocess_log_file(src_path: str, dst_path: str, prefix_regex: str) -> None:
    """
    预处理日志文件：
    - 根据前缀正则 prefix_regex，将被拆成多行的一条日志重新合并
    - 结果写入新的文件 dst_path，文件名与原文件相同、目录由调用方控制
    """
    from loguru import logger

    logger.info(f"开始预处理日志文件: {src_path}")

    with open(src_path, 'r', encoding='utf-8') as f_in:
        raw_lines = f_in.readlines()

    merged: List[str] = []
    current: str = ""

    pattern = re.compile(prefix_regex)

    for line in raw_lines:
        try:
            match = pattern.search(line)
        except Exception as e:
            logger.warning(f"前缀正则匹配出错: {e}")
            match = None

        if match:
            if current:
                # 确保每条日志以换行结尾
                if not current.endswith("\n"):
                    current += "\n"
                merged.append(current)
            current = line
        else:
            if current:
                current += line
            else:
                # 文件开头就不规整，直接视为一条日志
                current = line

    if current:
        if not current.endswith("\n"):
            current += "\n"
        merged.append(current)

    # 写入目标文件
    with open(dst_path, 'w', encoding='utf-8') as f_out:
        f_out.writelines(merged)

    logger.info(f"预处理完成：原始 {len(raw_lines)} 行，合并为 {len(merged)} 条日志，输出到 {dst_path}")



if __name__ == '__main__':
    # r = re_extractor(r'\[\d{2}/[A-Za-z]{3}/\d{4}:\d{2}:\d{2}:\d{2}', '192.168.2.102 - - [09/Sep/2025:10:00:02 +0800] "POST /gerrit/a/changes/14589/revisions/2/comments HTTP/1.1" 201 389 "https://gerrit.example.com/gerrit/#/c/14589/" "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) Safari/605.1.15" "172.16.3.102"')

    # print(r)
    x = """```json\n{\n    "result": r\'^\\[\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}\\] ref-updated (.*)$\'\n}\n```\n\n这个正则表达式可以解释为：\n1. `^` 匹配行首\n2. `\\[\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}\\]` 匹配时间戳部分，格式为 `[YYYY-MM-DD HH:MM:SS,SSS]`\n3. ` ref-updated ` 匹配固定的字符串\n4. `(.*)` 捕获分组，匹配时间戳和固定字符串之后的所有内容（即日志体部分）\n5. `$` 匹配行尾\n\n这个正则表达式会捕获每行日志中 `ref-updated` 之后的所有内容作为日志体返回。
"""
    print(response_extractor(x))


