# coding=utf-8
# @Time : 2025/3/3 19:36
# @Author : RoseLee
# @File : base
# @Project : fault-analysis
# @Description :
import re
from typing import Dict, Any, Optional

import requests
from openai import OpenAI, APIError, APIConnectionError
import copy
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)
import atexit
from loguru import logger

# 定义全局统计变量
total_requests = 0
total_prompt_tokens = 0
total_completion_tokens = 0
total_tokens = 0
class LLMClient:
    """大语言模型客户端

    Attributes:
        BASE_URL (str): 请求基础URL
        DEFAULT_TIMEOUT_SECONDS (int): 默认超时时间

    """
    BASE_URL: str = None

    # 默认超时时间 300s
    DEFAULT_TIMEOUT_SECONDS: int = 300

    DEFAULT_JSON_SCHEMA_PROMPT = """你的回答必须符合以下JSON Schema：\n\n```json\n{json_schema}\n```\n\n你绝不能在你的JSON回答中添加任何额外字段，也绝不能添加类似“这是你的JSON”这样的额外前言。"""

    def __init__(self, llm_config: Dict[str, Any], **kwargs):
        """
        Args:
            llm_config (dict): 大模型配置
                - provider: 大模型服务提供商
                - base_url (str): 请求基础URL
                - api_key (str): API KEY
                - temperature (float): 温度
                - model (str): 模型
                - timeout (int): 超时时间(s)
                - generate_config (dict): 生成配置

            kwargs: 暂定

        """
        llm_config = copy.deepcopy(llm_config)
        # 请求基础URL
        if "base_url" not in llm_config and self.BASE_URL is None:
            raise ValueError(f"`base_url` 必须在 config 或 {self.__class__.__name__} 类中指定")
        self._base_url = llm_config.get("base_url", self.BASE_URL)

        # API KEY
        if "api_key" not in llm_config:
            raise ValueError("`api_key` 必须在 config 中指定")
        self._api_key = llm_config.get("api_key")

        # 模型
        if "model" not in llm_config:
            raise ValueError("`model` 必须在 config 中指定")
        self._model = llm_config.get("model")
        # 超时时间
        self._timeout = llm_config.get("timeout", self.DEFAULT_TIMEOUT_SECONDS)
        # 生成配置
        self._generate_config = llm_config.get("generate_config", {})

        # 初始化 OpenAI 客户端
        self.client = OpenAI(
            api_key=self._api_key,
            base_url=self._base_url,
            timeout=self._timeout,
        )
    @retry(
        stop=stop_after_attempt(3),  # 最大重试次数
        wait=wait_exponential(multiplier=1, max=10),  # 指数退避等待
        retry=retry_if_exception_type((APIError, APIConnectionError)),  # 重试条件
    )
    def __retryable_chat_completion(self, **params):
        """带重试机制的聊天补全请求"""
        try:
            response = self.client.chat.completions.create(**params)
            logger.debug(f"LLM 请求成功: {params}")
            return response
        except (APIError, APIConnectionError) as e:
            logger.warning(f"LLM 请求失败（可重试）: {e}, params={params}")
            raise
        except Exception as e:
            logger.error(f"LLM 请求失败（不可重试）: {e}, params={params}")
            raise

    def infer(
            self,
            system_prompt: str,
            user_prompt: str,
            job_type = None,
            temperature: Optional[float] = None,
            max_tokens: Optional[int] = None,
    ):
        """
        向大模型发起对话（自动重试）

        Args:
            system_prompt: 系统提示词
            user_prompt: 用户输入
            temperature: 覆盖默认温度参数
            max_tokens: 覆盖默认最大token数

        Returns:
            str: 模型生成的回复内容

        Raises:
            APIError: OpenAI API 错误
            APIConnectionError: 网络连接错误
        """
        # 合并生成配置与覆盖参数
        # print(user_prompt)
        global total_requests, total_prompt_tokens, total_completion_tokens, total_tokens
        params = {
            "model": self._model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
                {"role": "assistant", "content": ""}
            ],
            "temperature": temperature or self._generate_config.get("temperature", 1.0),
            "max_tokens": max_tokens or self._generate_config.get("max_tokens", 8192),
            "stream": False,
        }

        try:
            response = self.__retryable_chat_completion(**params)
            logger.info("====================================")
            try:
                prompt_tokens = response.usage.prompt_tokens
            except Exception as e:
                print(response)
            completion_tokens = response.usage.completion_tokens
            total_usage_tokens = response.usage.total_tokens

            total_requests += 1
            total_prompt_tokens += prompt_tokens
            total_completion_tokens += completion_tokens
            total_tokens += total_usage_tokens

            logger.info(f"prompt的token用量为：{prompt_tokens}")
            logger.info(f"大模型回答token的用量为：{completion_tokens}")
            logger.info(f'输入输出总token用量为：{total_usage_tokens}')
            logger.info("====================================")


            if job_type == 'template':
                return modify_template(response.choices[0].message.content.strip())

            if job_type == 'workflow':
                return modify_workflow(response.choices[0].message.content.strip())
                pass

            return response.choices[0].message.content.strip()
        except Exception as e:
            logger.error(f"LLM 请求最终失败: {e}")
            raise

    def infer_with_assistant(
            self,
            system_prompt: str,
            user_prompt: str,
            assistant: list
    ):
        """
        附带上下文的对话
        :param system_prompt:
        :param user_prompt:
        :param assistant:
        :return:
        """
        global total_requests, total_prompt_tokens, total_completion_tokens, total_tokens
        message = assistant +[
            {"role":"system", "content":system_prompt},
            {"role":"user","content":user_prompt}
        ]
        params = {
            "model":self._model,
            "messages": message,
            "temperature":1.0,
            "stream":False
        }

        try:
            # response = self.__retryable_chat_completion(**params)
            response = self.__retryable_chat_completion(**params)
            print(response.get('choices')[0].get('message').get('content').strip())
            output = response.get('choices')[0].get('message').get('content').strip()
            result = re.sub(r'<think>[\s\S]*?</think>', '', output, count=1).strip()
            logger.info("====================================")
            prompt_tokens = response.usage.prompt_tokens
            completion_tokens = response.usage.completion_tokens
            total_usage_tokens = response.usage.total_tokens

            total_requests += 1
            total_prompt_tokens += prompt_tokens
            total_completion_tokens += completion_tokens
            total_tokens += total_usage_tokens

            logger.info(f"prompt的token用量为：{prompt_tokens}")
            logger.info(f"大模型回答token的用量为：{completion_tokens}")
            logger.info(f'输入输出总token用量为：{total_usage_tokens}')
            logger.info("====================================")
            assistant.append({"role": "assistant","content":output})
            descrip, chain = modify_workflow(result)
            return descrip, chain, assistant
        except Exception as e:
            logger.error(f"LLM 最终请求失败：{e}")
            raise


def modify_template(response):
    '''
    解析大模型日志解析返回结果
    :param response:
    :return: 若大模型解析成功，则返回[模板,True]，否则输出错误信息
    '''
    lines = response.split('\n')
    log_template = None
    for line in lines:
        if line.find("Log template:") != -1:
            log_template = line
            break
    if log_template is None:
        for line in lines:
            if line.find("`") != -1:
                log_template = line
                break
    if log_template is not None:
        start_index = log_template.find('`') + 1
        end_index = log_template.rfind('`')

        if start_index == 0 or end_index == -1:
            start_index = log_template.find('"') + 1
            end_index = log_template.rfind('"')

        if start_index != 0 and end_index != -1 and start_index < end_index:
            template = log_template[start_index:end_index]
            return template
    print("======================================")
    print("LLM response format error.log: ")
    print(response)
    print("======================================")
    return None

def modify_workflow(response):
    '''
    解析大模型的故障排查路径生成结果
    :param response: 大模型的原生生成结果
    :return: 返回一个数组，数组顺序为日志文件排查顺序
    '''

    description = ""
    chain = []

    lines = [line.strip() for line in response.split('\n') if line.strip()]

    for line in lines:
        if line.startswith("Reason:"):
            # 提取description
            description = line.split(':', 1)[1].strip()
        elif line.startswith("Check_Sequence:"):
            # 处理Check_Sequence部分
            content = line.split(':', 1)[1].strip()
            # 按分号分割并过滤空部分
            parts = [p.strip() for p in content.split(';') if p.strip()]
            for part in parts:
                elements = part.split('#')
                chain.append(elements)
    return description, chain

def print_statistics():
    global total_requests, total_prompt_tokens, total_completion_tokens, total_tokens
    logger.info("================ 统计信息 ================")
    logger.info(f"总请求次数: {total_requests}")
    logger.info(f"总prompt token用量: {total_prompt_tokens}")
    logger.info(f"总大模型回答token用量: {total_completion_tokens}")
    logger.info(f"总大模型使用token量: {total_tokens}")


# 注册 atexit 函数
# atexit.register(print_statistics)
if __name__ == '__main__':
    llm = LLMClient(llm_config = {
    "api_key": "sk-874d5743efa24640b6a639edad0005f4",
    "model": "deepseek-chat",
    "base_url": "https://api.deepseek.com",
    "generate_config": {
        "temperature": 0.4,
    }
})
    print(llm.infer(system_prompt='你是一个日志解析专家，你十分擅长进行日志模板解析。',
                    user_prompt='''
                    ## 任务描述
你是一位日志模板解析专家，我将给你一条使用``包裹的日志消息，你需要根据使用<占位符(*)>  标识并抽象出日志中所有的动态变量，并输出静态日志模板。打印以反引号``分割的输入日志模板。


下面是一个示例：
## 示例：
Log message: `try to connected to host: 172.16.254.1, finished.`
Log template: `try to connected to host: <*>, finished.`


## 任务
解析如下日志信息：
Log message: `<info>  [1737511578.2116] dhcp-init: Using DHCP client 'internal'`
                    ''', job_type='template'))



