# coding=utf-8
# @File : configs
# @Project : fault-analysis
# @Description : 配置文件信息

import os

# os.chdir('/home/6000015813/PycharmProjects/fault-analysis')
os.chdir('E:\\Self_code\\PycharmProjects\\fault-analysis')

TEMPLATE_PATH = "resource/lib/template/"
TEMPLATE_REGEX_PATH = "resource/lib/template/prefix_regex.csv"
PLACEHOLDER_LIMITS = ["IP地址", "代码仓库", "员工工号", "员工姓名"]

LLM_CONFIG = {
    # 大模型名称，例如: "gpt-4o-mini" / "deepseek-chat" / "model you set"
    "model": "model you set",
    # 模型服务地址，例如: "https://api.openai.com/v1"
    "base_url": "base url you set",
    # 访问密钥，例如: "sk-xxxx"
    "api_key": "api key you set",
    # 请求超时时间（秒）
    "timeout": 300,
    # 生成参数
    "generate_config": {
        # 采样温度，越高越发散
        "temperature": 0.7,
        # 单次生成的最大 token 数
        "max_tokens": 8192,
    },
    # 兼容部分自定义客户端可选请求头
    "header": {
        "Authorization": "Bearer token you set",
        "Content-Type": "application/json",
    },
}
