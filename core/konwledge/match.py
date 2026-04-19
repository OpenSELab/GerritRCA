# coding=utf-8
# @File : match
# @Project : fault-analysis
# @Description :
import os
import re
import json
import configs
import pandas as pd
from  sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from core.llm.base import LLMClient
from core.prompts.prompt_loader import PromptLoader

class Matcher:
    """
    实现任务流程中与库中知识匹配的操作
        1.实现故障信息与任务匹配
        2.实现故障相似案例查询
    """
    def __init__(self,
                 fault_description,
                 path
                 ):
        """

        :param fault_description:
        :param path:工作流库文件地址
        """
        self.llm = LLMClient(llm_config=configs.LLM_CONFIG)
        self.fault = fault_description
        self.path = path


    def match_job_llm(self):
        """使用大模型匹配任务

        :return: 目标任务
        """
        df = pd.read_csv(self.path)
        d = df[['id','job']]
        output_lines = []
        for _, row in d.iterrows():
            line = f'id:{row["id"]}, 任务为：{row["job"]}'
            output_lines.append(line)
        output = "\n".join(output_lines)

        result = self.llm.infer(
            system_prompt="你是一个任务匹配专家",
            user_prompt=PromptLoader.get_prompt(
                prompt_name='lib/job_match.prompt',
                joblib=output,
                fault_description=self.fault
            )
        )

        m = re.search(r"Job_Id:\s*(->\d+)", result)
        print(m)
        if m:
            try:
                i = int(m.group(1))
                job = d.loc[d['id'] == i, "job"].values[0]
                return job
            except:
                return "大模型返回结果有误"

        return "大模型返回结果有误"

    def match_job_sb(self, job, workflow_strorage_path):
        df_task = pd.read_csv(workflow_strorage_path, encoding='utf-8')
        model = SentenceTransformer('')

def match_by_similarity(str1, str2, limitation=0.8):
    model = SentenceTransformer('core/model')
    if str1 == '':
        return False, None, model.encode([str2])
    sentence_embeddings1 = model.encode([str1])
    sentence_embeddings2 = model.encode([str2])
    similarity = cosine_similarity(sentence_embeddings1, sentence_embeddings2)
    if similarity > limitation:
        return True, sentence_embeddings1, sentence_embeddings2
    else:
        return False, sentence_embeddings1, sentence_embeddings2

def match_by_similarity_vector(str1, vector, limitation=0.8):
    model = SentenceTransformer('core/model')
    sentence_embeddings1 = model.encode([str1])
    similarity = cosine_similarity(sentence_embeddings1, vector)
    if similarity > limitation:
        return True
    else:
        return False
if __name__ == '__main__':
    print(match_by_similarity("测试句子", "测试句子"))