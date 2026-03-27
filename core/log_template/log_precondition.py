# coding=utf-8
# @Time : 2025/3/3 19:04
# @Author : RoseLee
# @File : precondition
# @Project : fault-analysis
# @Description : 进行日志模板解析前的预处理:日志文件加载、日志前缀处理。
import re

import pandas as pd
from loguru import logger

import configs
from core.llm.base import LLMClient
from core.log_template.parsing_cache import ParsingCache
from core.prompts.prompt_loader import PromptLoader
from core.utils import response_extractor

class LogPrefixParser:
    '''
    进行日志前缀处理
    '''
    def __init__(self, prefix_regex):
        '''
        :param prefix_regex: 提前写好的日志前缀的正则表达式，存放在根目录下的configs.py中
        '''
        self.prefix_regex = re.compile(prefix_regex)

    def parse(self, log_line):
        match = self.prefix_regex.match(log_line)
        if match:
            info = list(match.groups())
            content = log_line[match.end():].strip()
            return info, content
        return [], None

    def content_parse(self, log_line):
        '''根据正则表达式解析日志内容'''
        match = self.prefix_regex.match(log_line)
        if match:
            info = list(match.groups())
            return info
        return None

def load_logs_file(lines, prefix_format):
    '''
    :param lines: 日志文件行
    :param prefix_format: 传入日志文件的前缀格式的对应正则表达式
    :return: 返回日志文件的前缀解析结果(前缀参数列和日志内容的字典)、前缀参数列的个数
    '''
    prefix_parser = LogPrefixParser(prefix_format)
    logs = []
    fail_log_line = []
    current_info = None
    current_content = None
    for line in lines:
        stripped_line = line.rsplit('\n')
        prefix, content = prefix_parser.parse(stripped_line)
        if content is not None:
            if current_info is not None:
                logs.append([current_info,current_content])
            # 开始新条目
            current_info = prefix
            current_content = content
        else:
            if current_info is not None:
                # 去除前后空白，非空内容才追加
                line_content = stripped_line.strip()
                if line_content:
                    current_content += ' ' + line_content

            else:# 无法识别的行
                print(f'当前日志行：{stripped_line},不符合预设格式')
                fail_log_line.append(stripped_line)

    # 处理最后缓存的日志条目
    if current_info is not None:
        logs.append([current_info, current_content])
    print(f'前缀匹配失败的日志行为：{fail_log_line}')
    return logs

def load_logs_file_by_path(path, prefix_format):
    '''
    :param path: 日志文件路径
    :param prefix_format: 传入日志文件的前缀格式的对应正则表达式
    :return: 返回日志文件的前缀解析结果(前缀参数列和日志内容的字典)、前缀参数列的个数
    '''
    prefix_parser = LogPrefixParser(prefix_format)
    logs = []
    fail_log_line = []
    current_info = None
    current_content = None
    with open(path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

        for line in lines:
            stripped_line = line.rsplit('\n')
            prefix, content = prefix_parser.parse(stripped_line)
            if content is not None:
                if current_info is not None:
                    logs.append([current_info,current_content])
                # 开始新条目
                current_info = prefix
                current_content = content
            else:
                if current_info is not None:
                    # 去除前后空白，非空内容才追加
                    line_content = stripped_line.strip()
                    if line_content:
                        current_content += ' ' + line_content

                else:# 无法识别的行
                    print(f'当前日志行：{stripped_line},不符合预设格式')
                    fail_log_line.append(stripped_line)

    # 处理最后缓存的日志条目
    if current_info is not None:
        logs.append([current_info, current_content])
    print(f'前缀匹配失败的日志行为：{fail_log_line}')
    return logs

def load_content(path, format):
    """
    对于可以直接解析的日志行进行加载
    :param path: 日志文件路径
    :param format: 日志内容的格式
    :return:
    """
    parser = LogPrefixParser(format)
    logs = []
    fail_log_line = []
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            variables = parser.content_parse(line)
            if variables is None:
                print(f"当前日志行:{line},不符合预设格式.")
                fail_log_line.append(line)
                continue
            # 将前缀解析结果和日志内容以字典形式保存，并返回
            logs.append([variables, line])
    print(f'前缀匹配失败的日志行为：{fail_log_line}')


    return logs
def log_templates_parsing(key_lines, prefix_format, prefix_parameters, llm_config, is_pasered, template_path=None):
    """
    进行日志文件的解析
    :param key_lines: 解析完成的日志行
    :param prefix_format: 前缀的正则表达式/日志的正则表达式
    :param prefix_parameters: 正则表达式解析结果对应的解析结果变量
    :param llm_config: 大模型配置
    :param out_path: 模板输出路径
    :param is_pasered: 进行日志解析/日志匹配
    :param template_path: 模板文件的路径
    :return:
    """
    # 定义日志缓存匹配
    # cache = ParsingCache()
    # 加载大模型
    # ds = LLMClient(llm_config=llm_config)

    # 标志位，True表示需要进行时间戳归一化，False表示不存在时间戳，无法进行时间戳归一化
    flag = True
    if is_pasered == 1:
        '''is_pasered == 1说明该日志文件有前缀'''
        # 定义前缀解析
        prefix_parse = load_logs_file(key_lines, prefix_format)
        # 使用预先定义的前缀变量
        prefix_parameters = prefix_parameters
        logs = []
        logger.info('日志解析---日志前缀解析完成（当前日志文件存在前缀）')
        # if template_path is not None:
        #     '''如果当前解析文件存在模板，先将其加载进模板缓存匹配树'''
        #     logger.info('日志解析---开始加载日志模板')
        #     df_tmp = pd.read_csv(template_path)
        #     logger.info("日志解析---开始加载库中已有日志模板")
        #     for index, row in df_tmp.iterrows():
        #         cache.add_templates(row['template'], insert=True)

        for line in prefix_parse:

            prefix, content = line
            # template, tid, parameter_str = cache.match_event(content)

            # 如果缓存匹配失败，则进行日志模板解析，并将模板解析结果存回日志缓存匹配树
            # if tid == "NoMatch":
            #     logger.info('日志解析---匹配失败，开始进行模板生成')
            #     template = ds.infer(system_prompt=PromptLoader.get_prompt("template/template_generate_system.prompt"),
            #                         user_prompt=PromptLoader.get_prompt("template/template_generate_user.prompt",
            #                         message=content), job_type='template'
            #                         )
            #     cache.add_templates(template, insert=True)
            #     logger.info(f'日志解析---模板生成结果为：{template}')
            #     template, tid, parameter_str = cache.match_event(content)

            # 模板匹配成功，生成日志的dataframe，以便后续处理

            x = dict(zip(prefix_parameters, prefix))
            timestamp = None
            try:
                date = x['date']
                time = x['time']
                timestamp = f'{date} {time}'.strip() if date is not None and time is not None else None
            except KeyError :
                logger.info('日志解析---该日志文件中不存在时间戳信息')
            if timestamp is None:
                flag = False
            if flag:
                logs.append({
                    'timestamp': timestamp,
                    # 'template': template,
                    'content': content,
                    # 'args': parameter_str,
                    'pre_args': x
                })
            else:
                logs.append({
                    # 'template': template,
                    'content': content,
                    # 'args': parameter_str,
                    'pre_args': x
                })

        # print(cache.template_list)
        logger.info("日志解析---模板解析完成")

        # df = pd.DataFrame(columns=['number', 'template'])
        # length = len(cache.template_list)
        # # 生成第一列的数据，从 E1 开始递增
        # first_column = [f'{i + 1}' for i in range(length)]
        # df['number'] = first_column
        # df['template'] = cache.template_list
        # df.to_csv(out_path, index=False)
        # print(df)
    else:
        '''说明当前日志没有前缀，不需要进行前缀解析,直接使用log_template的正则表达式进行匹配'''
        content_parse = load_content(key_lines, prefix_format)
        logger.info('日志解析---当前日志文件不存在日志前缀，直接开始解析...')
        logs = []
        for variables, line in content_parse:
            x = dict(zip(prefix_parameters, variables))
            date = x.get('date')  # 不存在则返回 None
            time = x.get('time')
            account = x.get('account')
            timestamp = f'{date} {time}'.strip() if date is not None and time is not None else None
            if timestamp is None:
                flag = False
            if flag:
                logs.append({
                    'timestamp': timestamp,
                    'account': account,
                    'content': line,
                    **x
                })
            else:
                logs.append({
                    'account': account,
                    'content': line,
                    **x
                })

    return logs

def prefix_format_test(path):
    """
    对输入的日志描述文件进行遍历，检查其中所有的日志前缀是否符合要求
    :param path:
    :return:
    """
    df = pd.read_csv(path)

    for index, row in df.iterrows():
        print(row['prefix_format'])
        if pd.isna(row['prefix_format']) or row['prefix_format'] == '':
            print(f"当前日志文件：{row['file_name']}还未进行日志解析")
            continue
        print("当前日志存在前缀，开始验证")
        # prefix_params = literal_eval(row['prefix_parameters'])
        # logs = log_templates_parsing()

def time_format_generate(logs: list):
    """
    为输入日志生成对应的时间格式
    """
    llm = LLMClient(configs.DEEPSEEK_CONFIG)
    if len(logs) > 5:
        log = logs[0:5]
    else:
        log = logs
    response = llm.infer(
            system_prompt="",
            user_prompt=PromptLoader.get_prompt(
                prompt_name="lib/time_format.prompt",
                logs=log
            ))
    # print(response)
    result = response_extractor(response).get('result')
    return result.get('regex'), result.get('datetime')



if __name__ == '__main__':
    ## 日志解析方法使用示例：调用load_logs_file，传参为：带解析日志文件路径、日志前缀的正则表达式
    ## 注意：正则表达式存档在/config.py中
    # logs = load_logs_file('../log/linux_log/Linux_2k.log', r'(\w+\s+\d+)\s+(\d{2}:\d{2}:\d{2})\s+(\w+)\s+([\w.() ]+)(?:\[(\d+)\])?\s*:')
    # logs = load_logs_file('../log/test_log/data.txt', r'^(\w+ \d{1,2}) (\d{2}:\d{2}:\d{2}) (\w+) ([\w./@-]+)(?:\[\d+\])?: ')
    # print(logs)

    # logs = load_content(path='E:\\Self_code\\PycharmProjects\\fault-analysis\\resource\\fake_log\\gerrit\\nginx\\access.log',
    #                     format='^(\S+) - (\S+) \[(\d{2}\/\w{3}\/\d{4}):(\d{2}:\d{2}:\d{2}) \+\d{4}\] "([^"]+)" (\d{3}) (\d+)')
    # logs = load_content(
    #     path='E:\\Self_code\\PycharmProjects\\fault-analysis\\resource\\fake_log\\gerrit\\nginx\\access.log',
    #     format=r'^(\S+) - (\S+) \[(\d{2}\/\w{3}\/\d{4}):(\d{2}:\d{2}:\d{2}) \+\d{4}\] "([^"]+)" (\d{3}) (\d+)')
    PromptLoader.from_paths(['../prompts'])
    logs = log_templates_parsing(
        log_file='E:\\Self_code\\PycharmProjects\\fault-analysis\\resource\\fake_log\\linux\\syslog',
        prefix_format='(\w{3} \d{1,2}) (\d{2}:\d{2}:\d{2}).*\[(\d+)\]:',
        out_path='E:\\Self_code\\PycharmProjects\\fault-analysis\\resource\\fake_template\\linux\\syslog.csv',
        llm_config=configs.DEEPSEEK_CONFIG,
        prefix_parameters=['date', 'time', 'pid'],
        is_pasered=1,
        template_path='E:\\Self_code\\PycharmProjects\\fault-analysis\\resource\\fake_template\\linux\\syslog.csv'
    )

    print(logs)
    # pass