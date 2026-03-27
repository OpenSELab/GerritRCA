# coding=utf-8
import csv
import json
import os
import random
import re
import sys
import time
from typing import List, Dict, Tuple, Any

import pandas

import configs
from core.llm.base import LLMClient
from core.prompts.prompt_loader import PromptLoader
from loguru import logger

from core.utils import response_extractor, re_extractor

sys.setrecursionlimit(1000000)
from datetime import datetime
import multiprocessing as mp
import string



def get_template_log(log_file_line, template_name):
    """根据模版匹配的对应的日志"""

    template_file_path = os.path.join(configs.TEMPLATE_PATH, template_name)
    logger.info("根据模版匹配对应的日志")
    template_regex_path = configs.TEMPLATE_REGEX_PATH
    # 读取当前日志的日志体匹配正则表达式
    df_regex = pandas.read_csv(template_regex_path)
    prefix_format = df_regex[df_regex['template_name'] == template_name]['regex'].values[0]

    # 先合并多行日志，再根据正则表达式匹配日志体
    # log_file_line = merge_multiline_logs(log_file_line, prefix_format)
    log_lines_body = []
    for i in log_file_line:
        log_body = re_extractor(prefix_format, i)
        try:
            log_lines_body.append(''.join(log_body))
        except:
            logger.info(f"{template_name}添加日志体：{log_body}出错")

    # 读取日志模版文件
    with open(template_file_path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        templates = [row[0].strip() for row in reader if row]

    template_and_log_compared =[]
    for t in templates:
        # 遍历日志模版，将所有日志加入树状解析器
        pc = ParsingCache()
        pc.add_templates(t)
        for i in log_lines_body:
            result = pc.match_event(i)
            if result[0] != 'NoMatch':
                template_and_log_compared.append([result[0], i])
                break
    return template_and_log_compared
def template_generating(llm_config, log_file_line, template_name, placeholder_limits=configs.PLACEHOLDER_LIMITS,log_content_description=''):
    """
    根据输入日志行与模板名在指定位置生成模板
    :param llm_config: 大模型配置
    :param log_file_line: 日志行，数组类型
    :param template_name: 模板名，单个模板名
    :param placeholder_limits: 占位符限定数组，例如 ["IP地址","代码仓库"]，如果为None则从configs中读取
    :return: (template_list_with_placeholders, parse_result)
    """


    logger.info('开始进行日志模版生成...')

    pc = ParsingCache()
    template_file_path = os.path.join(configs.TEMPLATE_PATH, template_name)
    template_regex_path = configs.TEMPLATE_REGEX_PATH
    llm = LLMClient(llm_config)
    
    # 用于存储模板和占位符解释的映射关系
    template_placeholders_map = {}

    # 若路径存在则将模版传入树状缓存
    if os.path.isfile(template_file_path):
        logger.info(f'当前日志文件存在历史解析模版:{template_name}，开始加载...')
        with open(template_file_path, 'r', newline='', encoding='utf-8')as f:
            reader = csv.reader(f)
            for row in reader:
                if row and row[0].strip():
                    template = row[0].strip()
                    # 读取占位符解释（如果有）
                    placeholders = row[1:] if len(row) > 1 else []
                    pc.add_templates(template)
                    if placeholders:
                        template_placeholders_map[template] = placeholders
    # 读取当前日志的日志体匹配正则表达式
    df_regex = pandas.read_csv(template_regex_path)
    flag = False
    save_flag = False
    if template_name in df_regex['template_name'].values:
        # 存在正则表达式就不用额外生成了
        logger.info('存在已经解析完成的正则表达式，直接加载...')
        prefix_format = df_regex[df_regex['template_name']==template_name]['regex'].values[0]
    else:
        # 生成前缀格式匹配日志体
        logger.info(f"开始为生成：{template_name}匹配日志体的正则表达式...")
        flag = True
        count = 1
        prefix_format = ''
        while flag:
            logger.info(f"开始第{count}次生成")
            response = llm.infer(
                system_prompt='你是一个日志解析助手，你需要根据传入日志生成对应的正则表达式',
                user_prompt=PromptLoader.get_prompt(
                    prompt_name='template/log_whole_prefix.prompt',
                    log=log_file_line[0:10]
                )
            )
            prefix_format = response_extractor(response).get('result')
            logger.info(f'生成结果为：{prefix_format}')
            logger.info(f'开始测试所生成正则表达式：{prefix_format}的正确性')
            s = random.randint(0, 10)
            log_body = re_extractor(prefix_format, log_file_line[s])
            try:
                logger.info(f"{log_body[0]}")
            except Exception as e:
                logger.warning('正则表达式匹配错误重新生成')
                count +=1
                continue
            flag = False
            logger.success(f"正则表达式：{prefix_format}验证成功")
        new_row = {'template_name':template_name, 'regex': prefix_format}
        df_regex = df_regex._append(new_row, ignore_index=True)
    # 根据正则表达式匹配日志体
    log_lines_body = []
    original_log_map = {}  # {log_body: original_log} 保存原始日志行和日志体的对应关系
    logger.info(f"根据正则表达式:{prefix_format}匹配日志体")
    for original_log in log_file_line:
        try:
            log_body_result = re_extractor(prefix_format, original_log)
            if log_body_result:
                log_body = ''.join(log_body_result)
                log_lines_body.append(log_body)
                original_log_map[log_body] = original_log
        except:
            # 如果提取失败，使用原始日志作为日志体
            log_lines_body.append(original_log)
            original_log_map[original_log] = original_log
            logger.info(f"无法提取日志体，使用原始日志: {original_log[:50]}...")

    # 保存匹配结果
    parse_result = []
    # 使用缓存树进行匹配，匹配失败则进行模版生成
    
    for line in log_lines_body:
        template, template_id, parameter_str = pc.match_event(line)
        if template == "NoMatch":
            save_flag = True
            logger.warning('未找到日志模板，开始调用大语言模型进行日志模板生成....')
            re_generating = True
            assistant = ''
            count = 1
            while re_generating:
                if count>10:
                    logger.error(f"日志：{line}，生成日志模板失败")
                    break
                logger.info(f'开始第{count}次生成')
                count+=1
                new_log_template_json = llm.infer(
                    system_prompt='',
                    user_prompt=PromptLoader.get_prompt(
                        "template/template_generating.prompt",
                        log_line=line,
                        placeholder_limits=placeholder_limits,
                        assistant=assistant,
                        log_content_description=log_content_description
                    )
                )
                new_log_template = response_extractor(new_log_template_json)
                template_str = new_log_template.get("result")
                placeholders = new_log_template.get("placeholders", [])
                
                # 验证占位符数量是否匹配
                placeholder_count = template_str.count("<*>")
                if len(placeholders) != placeholder_count:
                    logger.warning(f"占位符解释数量({len(placeholders)})与模板中占位符数量({placeholder_count})不匹配，重新生成")
                    re_generating = True
                    assistant +=f"上次生成的结果中，模板：{template_str}的占位符解释：{placeholders}，可以看到占位符解释的数量（{len(placeholders)}）与模板中占位符数量（{placeholder_count}）不匹配，请重新生成并保证占位符数量相同"
                    continue
                
                # 保存模板和占位符解释的映射关系
                template_placeholders_map[template_str] = placeholders
                
                print("==========新增日志模板进日志模板缓存树==========")
                pc.add_templates(template_str)
                template, template_id, parameter_str = pc.match_event(line)
                if template == "NoMatch":
                    re_generating = True
                    continue
                else:
                    re_generating = False
                    break

        # 使用原始日志行而不是日志体
        original_log = original_log_map.get(line, line)
        parse_result.append([original_log, template] + list(parameter_str))
        logger.info(template, parameter_str)
        logger.info(f"成功匹配：{line}")

    # 将匹配结果存放至对应路径下
    # with open(save_path, 'w', newline='')as f:
    #     writer = csv.writer(f)
    #     for item in parse_result:
            # writer.writerow(item)
    # 若进行日志模版新增则保存日志解析模版
    if save_flag:
        with open(template_file_path, 'w', newline='', encoding='utf-8')as f:
            writer = csv.writer(f)
            for item in pc.template_list:
                # 从映射关系中获取占位符解释，如果没有则使用默认名称
                placeholders = template_placeholders_map.get(item, [])
                if not placeholders:
                    placeholder_count = item.count("<*>")
                    placeholders = [f"占位符{i+1}" for i in range(placeholder_count)]
                writer.writerow([item] + placeholders)
            logger.success(f'检测到日志模版更新，将更新后的日志模版保存至{template_file_path}')

    # 保存正则表达式
    df_regex.to_csv(template_regex_path, index=False)
    logger.info("日志模版匹配及解析完成")
    
    # 构建返回的模板列表，格式为 [[模板,占位符1的解释,占位符2的解释....],...]
    # 需要读取模板文件中的所有模板（包括已存在的和新增的）
    template_list_with_placeholders = []
    if os.path.isfile(template_file_path):
        with open(template_file_path, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                if row and row[0].strip():
                    template = row[0].strip()
                    placeholders = row[1:] if len(row) > 1 else []
                    template_list_with_placeholders.append([template] + placeholders)
    else:
        # 如果文件不存在，从 template_placeholders_map 构建
        for item in pc.template_list:
            placeholders = template_placeholders_map.get(item, [])
            if not placeholders:
                placeholder_count = item.count("<*>")
                placeholders = [f"占位符{i+1}" for i in range(placeholder_count)]
            template_list_with_placeholders.append([item] + placeholders)
    
    return template_list_with_placeholders, parse_result


def match_logs_by_templates(log_lines: List[str], template_file_path: str, prefix_format: str = None):
    """
    根据模板文件匹配日志行，返回匹配结果

    :param log_lines: 日志行列表
    :param template_file_path: 模板文件路径（CSV格式，第一列为模板，后续列为占位符解释）
    :param prefix_format: 日志前缀正则表达式（可选，用于提取日志体）
    :return: 匹配结果列表，每个元素为字典，包含日志行、模板、参数列表、占位符位置列表
    """
    logger.info(f"开始使用模板文件 {template_file_path} 匹配日志")

    # 读取模板文件
    templates_with_placeholders = []
    if os.path.isfile(template_file_path):
        with open(template_file_path, 'r', newline='', encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                if row and row[0].strip():
                    template = row[0].strip()
                    placeholders = row[1:] if len(row) > 1 else []
                    templates_with_placeholders.append({
                        'template': template,
                        'placeholders': placeholders
                    })
    else:
        logger.warning(f"模板文件不存在: {template_file_path}")
        return []

    # 初始化解析缓存
    pc = ParsingCache()
    for item in templates_with_placeholders:
        pc.add_templates(item['template'])

    # 如果提供了前缀格式，先提取日志体
    if prefix_format:
        log_lines_body = []
        for line in log_lines:
            try:
                log_body = re_extractor(prefix_format, line)
                if log_body:
                    log_lines_body.append((line, log_body[0]))
            except:
                log_lines_body.append((line, line))
    else:
        log_lines_body = [(line, line) for line in log_lines]

    # 匹配日志
    match_results = []
    for original_line, log_body in log_lines_body:
        template, template_id, parameter_str = pc.match_event(log_body)
        if template != "NoMatch":
            # 找到对应的占位符解释
            placeholders = []
            for item in templates_with_placeholders:
                if item['template'] == template:
                    placeholders = item['placeholders']
                    break

            # 计算占位符位置
            placeholder_positions = []
            if isinstance(parameter_str, tuple):
                parameter_list = list(parameter_str)
            else:
                parameter_list = [parameter_str] if parameter_str else []

            # 占位符位置是在参数列表中的索引（从0开始）
            for placeholder_idx in range(len(parameter_list)):
                placeholder_positions.append({
                    'position': placeholder_idx,  # 占位符在参数列表中的位置索引
                    'value': parameter_list[placeholder_idx],
                    'description': placeholders[placeholder_idx] if placeholder_idx < len(
                        placeholders) else f'占位符{placeholder_idx + 1}'
                })

            match_results.append({
                'log_line': original_line,
                'template': template,
                'template_id': template_id,
                'parameters': parameter_list,
                'placeholder_positions': placeholder_positions,
                'placeholders': placeholders
            })

    logger.info(f"匹配完成，共匹配 {len(match_results)} 条日志")
    return match_results
def print_tree(move_tree, indent=' '):
    for key, value in move_tree.items():
        if isinstance(value, dict):
            print(f'{indent}|- {key}')
            print_tree(value, indent + '|  ')
        elif isinstance(value, tuple):
            print(f'{indent}|- {key}: tuple')
        else:
            print(f'{indent}|- {key}: {value}')


def lcs_similarity(X, Y):
    m, n = len(X), len(Y)
    c = [[0] * (n + 1) for _ in range(m + 1)]
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            if X[i - 1] == Y[j - 1]:
                c[i][j] = c[i - 1][j - 1] + 1
            else:
                c[i][j] = max(c[i][j - 1], c[i - 1][j])
    return 2 * c[m][n] / (m + n)

class ParsingCache(object):
    def __init__(self):
        self.template_tree = {}
        self.template_list = []
    
    def add_templates(self, event_template, insert=True, relevant_templates=[]):

            # if "<*>" not in event_template:
            #     self.template_tree["$CONSTANT_TEMPLATE$"][event_template] = event_template
            #     continue
            # original_template = event_template
            # event_template = self._preprocess_template(event_template)
            #print("event template after preprocess: ", event_template)
        template_tokens = message_split(event_template)
        if not template_tokens or event_template == "<*>":
            return -1
        if insert or len(relevant_templates) == 0:
            id = self.insert(event_template, template_tokens, len(self.template_list))
            self.template_list.append(event_template)
            return id
        # print("relevant templates: ", relevant_templates)
        max_similarity = 0
        similar_template = None
        for rt in relevant_templates:
            splited_template1, splited_template2 = rt.split(), event_template.split()
            if len(splited_template1) != len(splited_template2):
                continue 
            similarity = lcs_similarity(splited_template1, splited_template2)
            if similarity > max_similarity:
                max_similarity = similarity
                similar_template = rt
        if max_similarity > 0.8:
            success, id = self.modify(similar_template, event_template)
            if not success:
                id = self.insert(event_template, template_tokens, len(self.template_list))
                self.template_list.append(event_template)
            return id
        else:
            id = self.insert(event_template, template_tokens, len(self.template_list))
            self.template_list.append(event_template)
            return id
            #print("template tokens: ", template_tokens)
            
    def insert(self, event_template, template_tokens, template_id):
        start_token = template_tokens[0]
        if start_token not in self.template_tree:
            self.template_tree[start_token] = {}
        move_tree = self.template_tree[start_token]

        tidx = 1
        while tidx < len(template_tokens):
            token = template_tokens[tidx]
            if token not in move_tree:
                move_tree[token] = {}
            move_tree = move_tree[token]
            tidx += 1

        move_tree["".join(template_tokens)] = (
            sum(1 for s in template_tokens if s != "<*>"),
            template_tokens.count("<*>"),
            event_template,
            template_id
        )  # statistic length, count of <*>, original_log, template_id
        return template_id

    def modify(self, similar_template, event_template):
        merged_template = []
        similar_tokens = similar_template.split()
        event_tokens = event_template.split()
        i = 0
        print(similar_template)
        print(event_template)
        for token in similar_tokens:
            print(token, event_tokens[i])
            if token == event_tokens[i]:
                merged_template.append(token)
            else:
                merged_template.append("<*>")
            i += 1
        merged_template = " ".join(merged_template)
        print("merged template: ", merged_template)
        success, old_ids = self.delete(similar_template)
        if not success:
            return False, -1
        self.insert(merged_template, message_split(merged_template), old_ids)
        self.template_list[old_ids] = merged_template
        return True, old_ids
        
    
    def delete(self, event_template):
        template_tokens = message_split(event_template)
        start_token = template_tokens[0]
        if start_token not in self.template_tree:
            return False, []
        move_tree = self.template_tree[start_token]

        tidx = 1
        while tidx < len(template_tokens):
            token = template_tokens[tidx]
            if token not in move_tree:
                return False, []
            move_tree = move_tree[token]
            tidx += 1
        old_id = move_tree["".join(template_tokens)][3]
        del move_tree["".join(template_tokens)]
        return True, old_id


    def match_event(self, log):
        return tree_match(self.template_tree, log)


    def _preprocess_template(self, template):
        # template = re.sub("<NUM>", "<*>", template)
        # if template.count("<*>") > 50:
        #     first_start_pos = template.index("<*>")
        #     template = template[0 : first_start_pos + 3]
        return template


    def printout_tree(self):
        print_tree(self.template_tree)

def post_process_tokens(tokens, punc):
    excluded_str = ['=', '|', '(', ')', '.', '-', ' :', '[', ']']
    for i in range(len(tokens)):
        if tokens[i].find("<*>") != -1:
            tokens[i] = "<*>"
        else:
            new_str = ""
            for s in tokens[i]:
                if (s not in punc and s != ' ') or s in excluded_str:
                    new_str += s
            tokens[i] = new_str
    return tokens

#splitter_regex = re.compile("(<\*>|[^A-Za-z])")
def message_split(message):
    #print(string.punctuation)
    punc = "!\"#$%&'()+,-/:;=?@.[\]^_`{|}~"
    #print(punc)
    #punc = re.sub("[*<>\.\-\/\\]", "", string.punctuation)
    splitters = "\s\\" + "\\".join(punc)
    #print(splitters)
    #splitters = "\\".join(punc)
    # splitter_regex = re.compile("([{}]+)".format(splitters))
    splitter_regex = re.compile("([{}])".format(splitters))
    tokens = re.split(splitter_regex, message)

    tokens = list(filter(lambda x: x != "", tokens))
    
    #print("tokens: ", tokens)
    tokens = post_process_tokens(tokens, punc)

    tokens = [
        token.strip()
        for token in tokens
        if token != "" and token != ' ' 
    ]
    tokens = [
        token
        for idx, token in enumerate(tokens)
        if not (token == "<*>" and idx > 0 and tokens[idx - 1] == "<*>")
    ]
    #print("tokens: ", tokens)
    #tokens = [token.strip() for token in message.split()]
    #print(tokens)
    return tokens



def tree_match(match_tree, log_content):

    log_tokens = message_split(log_content)
        #print("log tokens: ", log_tokens)
    template, template_id, parameter_str = match_template(match_tree, log_tokens)
    if template:
        return (template, template_id, parameter_str)
    else:
        return ("NoMatch", "NoMatch", parameter_str)


def match_template(match_tree, log_tokens):
    results = []
    find_results = find_template(match_tree, log_tokens, results, [], 1)
    relevant_templates = find_results[1]
    if len(results) > 1:
        new_results = []
        for result in results:
            if result[0] is not None and result[1] is not None and result[2] is not None:
                new_results.append(result)
    else:
        new_results = results
    if len(new_results) > 0:
        if len(new_results) > 1:
            new_results.sort(key=lambda x: (-x[1][0], x[1][1]))
        return new_results[0][1][2], new_results[0][1][3], new_results[0][2]
    return False, False, relevant_templates


def get_all_templates(move_tree):
    result = []
    for key, value in move_tree.items():
        if isinstance(value, tuple):
            result.append(value[2])
        else:
            result = result + get_all_templates(value)
    return result


def find_template(move_tree, log_tokens, result, parameter_list, depth):
    flag = 0 # no futher find
    if len(log_tokens) == 0:
        for key, value in move_tree.items():
            if isinstance(value, tuple):
                result.append((key, value, tuple(parameter_list)))
                flag = 2 # match
        if "<*>" in move_tree:
            parameter_list.append("")
            move_tree = move_tree["<*>"]
            if isinstance(move_tree, tuple):
                result.append(("<*>", None, None))
                flag = 2 # match
            else:
                for key, value in move_tree.items():
                    if isinstance(value, tuple):
                        result.append((key, value, tuple(parameter_list)))
                        flag = 2 # match
        # return (True, [])
    else:
        token = log_tokens[0]

        relevant_templates = []
        
        if token in move_tree:
            find_result = find_template(move_tree[token], log_tokens[1:], result, parameter_list,depth+1)
            if find_result[0]:
                flag = 2 # match
            elif flag != 2:
                flag = 1 # futher find but no match
                relevant_templates = relevant_templates + find_result[1]
        if "<*>" in move_tree:
            if isinstance(move_tree["<*>"], dict):
                next_keys = move_tree["<*>"].keys()
                next_continue_keys = []
                for nk in next_keys:
                    nv = move_tree["<*>"][nk]
                    if not isinstance(nv, tuple):
                        next_continue_keys.append(nk)
                idx = 0
                # print("len : ", len(log_tokens))
                while idx < len(log_tokens):
                    token = log_tokens[idx]
                    # print("try", token)
                    if token in next_continue_keys:
                        # print("add", "".join(log_tokens[0:idx]))
                        parameter_list.append("".join(log_tokens[0:idx]))
                        # print("End at", idx, parameter_list)
                        find_result = find_template(
                            move_tree["<*>"], log_tokens[idx:], result, parameter_list,depth+1
                        )
                        if find_result[0]:
                            flag = 2 # match
                        elif flag != 2:
                            flag = 1 # futher find but no match
                            # relevant_templates = relevant_templates + find_result[1]
                        if parameter_list:
                            parameter_list.pop()
                    idx += 1
                if idx == len(log_tokens):
                    parameter_list.append("".join(log_tokens[0:idx]))
                    find_result = find_template(
                        move_tree["<*>"], log_tokens[idx + 1 :], result, parameter_list,depth+1
                    )
                    if find_result[0]:
                        flag = 2 # match
                    else:
                        if flag != 2:
                            flag = 1
                        relevant_templates = relevant_templates + find_result[1]
                    if parameter_list:
                        parameter_list.pop()
    if flag == 2:
        return (True, [])
    if flag == 1:
        return (False, relevant_templates)
    if flag == 0:
        # print(log_tokens, flag)
        if depth >= 2:
            return (False, get_all_templates(move_tree))
        else:
            return (False, [])



if __name__ == '__main__':
    cache = ParsingCache()
    cache.add_templates("Jun <*> <*> combo sshd(pam_unix)[<*>]: check pass; user unknown")
    print(cache.match_event("Jun 14 15:16:02 combo sshd(pam_unix)[19937]: check pass; user unknown"))
    print(cache.match_event("Jun 14 15:16:02 cg5ggg"))
    print(cache.template_list)