# coding=utf-8
# @Time : 2025/2/5 9:27
# @Author : RoseLee
# @File : configs
# @Project : fault-analysis
# @Description : 配置文件信息

import os

# os.chdir('/home/6000015813/PycharmProjects/fault-analysis')
os.chdir('E:\\Self_code\\PycharmProjects\\fault-analysis')

TEMPLATE_PATH = "resource/lib/template/"
TEMPLATE_REGEX_PATH = "resource/lib/template/prefix_regex.csv"
PLACEHOLDER_LIMITS = ["IP地址", "代码仓库", "员工工号", "员工姓名"]

DEEPSEEK_CONFIG = {
    "model": "DeepSeek-V3",
    "base_url": "https://maas-apigateway.dt.zte.com.cn/model/deepseek-671b/v1/chat/completions",
    "header": {
        'Authorization': 'Bearer f1d9f9547ace4f2fbe06c38f9e771168',
        'Content-Type': 'application/json',
    }
}

R1_CONFIG = {
    "api_key": "sk-874d5743efa24640b6a639edad0005f4",
    "model": "deepseek-reasoner",
    "base_url": "https://api.deepseek.com",
    "generate_config": {
        "temperature": 1,
    }
}

ZTE_CONFIG = {
    "model": "DeepSeek-R1-XYDT",
    "base_url": "https://icosg.dt.zte.com.cn/STREAM/iaab/platform/openapi/v2/chat/completions",
    "header": {
        'Authorization': 'Bearer e984d64fa7994365afc3a5e312606afa-prod_b3ef91068e5c451b8dfa618b7cbdeb6b',
        'X-Emp-No': '6000015813',
        'X-Auth-Value': '8052a475e89c7f7bca2bcbe50a0ad0e1',
        'Content-Type': 'application/json',
        'appCode': '645d22494464fbfb44db45bf19fe05a'
    }
}

ZTE_V3_CONFIG = {
    "model": "DeepSeek-V3",
    "base_url": "https://maas-apigateway.dt.zte.com.cn/model/deepseek-671b/v1/chat/completions",
    "header": {
        'Authorization': 'Bearer f1d9f9547ace4f2fbe06c38f9e771168',
        'Content-Type': 'application/json',
    }
}
