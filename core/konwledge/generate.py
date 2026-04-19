# coding=utf-8
# @File : generate
# @Project : fault-analysis
# @Description :
"""
以日志数据为输入，输出知识库

"""
import random
import pandas as pd
import configs
import os
from pathlib import Path
from loguru import logger
from core.llm.base import LLMClient
from core.log_template.log_precondition import load_logs_file_by_path, load_logs_file
from core.log_template.parsing_cache import template_generating, get_template_log
from core.prompts.prompt_loader import PromptLoader
from core.utils import response_extractor, preprocess_log_file
from core.konwledge.knowledge_extract import extract_normal_log_cluster
from typing import List, Dict


class LogDescriptionFileGenerator:
    """
    根据日志数据生成对应的解释行,存放在日志描述文件中
    """
    def __init__(self, path, llm_config):
        # 初始化先定义描述文件的存储位置
        self.path = path
        self.df = pd.read_csv(path)
        # 如果存在 id 列，将其设置为 index
        if 'id' in self.df.columns:
            self.df = self.df.set_index('id')

        # 传入大模型客户端
        self.llm_config = llm_config
        self.llm = LLMClient(self.llm_config)



    def prefix_generate(self, line, failure_log=[],prefix_regex=None, order=None, datetime_format=None ):
        """
        根据日志内容生成前缀静态信息，包括前缀的正则表达式、时间属性变量排列、时间戳的datetime标准格式
        :return:
        """
        if len(failure_log) > 0:
            # 说明当前已经生成过一次但是格式有错误
            regex_order = self.llm.infer(
                system_prompt="",
                user_prompt=PromptLoader.get_prompt(
                    prompt_name="lib/log_perfix_regex_wrong.prompt",
                    log=line,
                    prefix_regex=prefix_regex,
                    order=order,
                    datetime_format=datetime_format,
                    failure_log=failure_log
                )
            )
        else:
            regex_order = self.llm.infer(
                system_prompt="",
                user_prompt=PromptLoader.get_prompt(
                    prompt_name="lib/log_perfix_regex.prompt",
                    log=line
                )
            )
        result = response_extractor(regex_order).get('result')
        return result.get('regex'), result.get('order'), result.get('datetime_format')

    def filter_factor_generate_from_log_template(self, log_file_path, platform, description):
        with  open(log_file_path, 'r')as f:
            lines = f.readlines()
        log_name = Path(log_file_path).name
        log_name_no_fix, old_ext = os.path.splitext(log_name)
        template_name = f'{log_name_no_fix}_templates.csv'
        # 获取日志模版，template_generating 会自动保存到 configs.TEMPLATE_PATH
        template_list, parse_result = template_generating(
            llm_config=self.llm_config,
            log_file_line=lines,
            template_name=template_name
        )
        # 利用日志模版匹配对应日志
        template_with_log = get_template_log(lines, template_name)
        response = self.llm.infer(
            system_prompt='',
            user_prompt=PromptLoader.get_prompt(
                prompt_name='template/filter_factor_generating.prompt',
                template_with_log=template_with_log,
                log_name=log_name,
                platform=platform,
                description=description
            )
        )

        factors = response_extractor(response).get('result')
        return factors

    def generate(self, log_file_path, platform,  sub_platform=None, log_metadata_row_name=-1):

        """根据输入的日志文件路径进行生成"""
        if not os.path.isfile(log_file_path):
            logger.error("传入日志路径有误")
            return None
        log_file_name = Path(log_file_path).name
        with open(log_file_path, 'r')as f:
            lines = list(f)
            log_lines_to_parse = random.sample(lines, k=min(20, len(lines)))
            log_lines_to_verify= random.sample(lines, k=min(20, len(lines)))

        # 设置循环停止标志
        flag = True
        # 若之前日志文件的元数据信息已经存在则传入行号进行修改，否则新增到最后
        if log_metadata_row_name == -1:
            # 判断当前dataframe的最后一行，以进行新增
            if self.df.empty:
                row_index = 1
            else:
                # row_index = self.df.iloc[:, -1].max() + 1
                row_index = self.df.index.max() + 1
            # self.df[row_index] = dict(zip(self.df.columns.tolist(), [""]*len(self.df.columns.tolist())))
                self.df.loc[row_index] = [""] * len(self.df.columns)
        else:
            row_index = log_metadata_row_name

        prefix_regex, order, datetime_format = self.prefix_generate(log_lines_to_verify)
        while flag:
            # 使用日志解析工具检测上方生成的`前缀正则表达式`等是否正确,若解析结果还有错误日志则重新生成
            logs, failure_logs = load_logs_file_by_path(log_file_path, prefix_regex)
            if len(failure_logs) == 0:
                #  不存在匹配不成功的情况则保存至dataframe
                flag = False
            else:
                # 存在匹配不成功情况则将匹配失败情况保存至dataframe中
                prefix_regex, order, datetime_format = self.prefix_generate(log_lines_to_parse, failure_logs, prefix_regex, order, datetime_format )
                continue

            # 验证通过才会执行以下保存内容
            self.df.loc[row_index, 'prefix_format'] = prefix_regex
            if order == 1:
                self.df.loc[row_index, 'prefix_parameters'] = str(['date', 'time'])
            else:
                self.df.loc[row_index, 'prefix_parameters'] = str(['time', 'date'])
            self.df.loc[row_index, 'time_format'] = datetime_format
        self.df.loc[row_index, 'platform'] = platform
        self.df.loc[row_index, 'sub_platform'] = sub_platform
        self.df.loc[row_index, 'file_name'] = log_file_name
        self.df.loc[row_index, 'path'] = log_file_path
        self.df.loc[row_index, 'is_pasered'] = 1
        self.df.loc[row_index, 'prompt_message'] = ''
        self.df.loc[row_index, 'div'] = 1
        
        # 自动生成日志描述
        logger.info(f"开始自动生成日志文件 {log_file_name} 的描述信息...")
        try:
            # 对日志进行分段采样
            total_lines = len(lines)
            sample_segments = 5  # 采样段数
            lines_per_segment = 6  # 每个段的日志行数
            
            log_samples = []
            if total_lines > 0:
                # 计算采样间隔
                if total_lines <= sample_segments * lines_per_segment:
                    # 如果日志行数不足，直接使用所有日志
                    log_samples = lines
                else:
                    # 均匀采样多个段
                    step = total_lines // sample_segments
                    for i in range(sample_segments):
                        start_idx = i * step
                        end_idx = min(start_idx + lines_per_segment, total_lines)
                        segment = lines[start_idx:end_idx]
                        log_samples.extend(segment)
            
            # 调用LLM生成描述
            description_prompt = PromptLoader.get_prompt(
                'lib/log_description_generate.prompt',
                log_samples='\n'.join(log_samples[:30]),  # 限制长度，避免prompt过长
                platform=platform,
                sub_platform=sub_platform if sub_platform else "无"
            )
            
            description_response = self.llm.infer(
                system_prompt="你是一个日志分析专家，能够根据日志内容生成准确的日志描述信息",
                user_prompt=description_prompt
            )
            
            description_result = response_extractor(description_response)
            description = description_result.get('description', '')
            
            if description:
                self.df.loc[row_index, 'description'] = description
                logger.info(f"自动生成的日志描述: {description}")
            else:
                logger.warning("LLM未能生成有效的日志描述，使用手动输入")
                self.df.loc[row_index, 'description'] = input(f'请输入当前日志文件：{log_file_path}的存储内容信息')
        except Exception as e:
            logger.error(f"自动生成日志描述失败: {e}，使用手动输入")
            import traceback
            traceback.print_exc()
            self.df.loc[row_index, 'description'] = input(f'请输入当前日志文件：{log_file_path}的存储内容信息')
        
        # 手动输入额外提示信息
        self.df.loc[row_index, 'prompt_message'] = input(f'请输入当前日志文件：{log_file_path}的额外提示信息')

        # 完成查询因子生成
        factors = self.filter_factor_generate_from_log_template(
            log_file_path=log_file_path,
            platform=platform,
            description=self.df.loc[row_index, 'description']
        )
        self.df.loc[row_index, 'filter_factor'] = str(factors)



    def save(self):
        """将当前dataframe保存至原文件"""
        # 如果 id 是 index，需要将其重置为普通列后再保存
        df_to_save = self.df.copy()
        if df_to_save.index.name == 'id' or (df_to_save.index.name is None and 'id' not in df_to_save.columns):
            # id 是 index，需要重置为普通列
            df_to_save = df_to_save.reset_index()
        df_to_save.to_csv(self.path, index=False)


class CrossSystemTaskWorkflowGenerator:
    """
    根据日志文件夹自动生成跨系统任务工作流文件 (cross_system_task_workflow.csv)
    包含字段：id, job, workchain, prompt_message, calling_seq
    """
    
    def __init__(self, save_path: str):
        """
        初始化生成器
        :param save_path: CSV文件保存路径
        """
        self.save_path = save_path
        # 初始化 DataFrame，如果文件已存在则读取，否则创建新的
        if os.path.isfile(save_path):
            try:
                self.df = pd.read_csv(save_path, encoding='utf-8')
                # 确保必要的列存在
                required_columns = ['id', 'job', 'workchain', 'prompt_message', 'calling_seq']
                for col in required_columns:
                    if col not in self.df.columns:
                        self.df[col] = ''
                logger.info(f"已加载现有文件: {save_path}，当前有 {len(self.df)} 条记录")
            except Exception as e:
                logger.warning(f"读取现有文件失败: {e}，将创建新的 DataFrame")
                self.df = pd.DataFrame(columns=['id', 'job', 'workchain', 'prompt_message', 'calling_seq'])
        else:
            # 创建新的 DataFrame
            self.df = pd.DataFrame(columns=['id', 'job', 'workchain', 'prompt_message', 'calling_seq'])
            logger.info(f"创建新的 DataFrame，保存路径: {save_path}")
    
    def generate(self, log_folder_path: str, start_time: str, end_time: str, lib: str, 
                 sample_points: int, llm_config: dict, input_file: List[str]):
        """
        根据日志文件夹生成跨系统任务工作流
        :param log_folder_path: 日志文件夹路径
        :param start_time: 开始时间，格式为 'YYYY-MM-DD HH:MM:SS'
        :param end_time: 结束时间，格式为 'YYYY-MM-DD HH:MM:SS'
        :param lib: 日志元数据文件路径
        :param sample_points: 采样点数量（每个采样点对应一个3秒的时间窗口）
        :param llm_config: 大模型配置
        :param input_file: 入口日志文件列表，例如 ['https_access.log', 'ssh_access.log']
        :return: None
        """
        if not os.path.isdir(log_folder_path):
            logger.error(f"日志文件夹路径不存在: {log_folder_path}")
            return None
        
        logger.info(f"开始从日志文件夹 {log_folder_path} 提取正常日志簇")
        logger.info(f"时间范围: {start_time} - {end_time}")
        logger.info(f"采样点数量: {sample_points}")
        logger.info(f"入口日志文件: {input_file}")
        
        # 调用 extract_normal_log_cluster 提取任务流程
        workflow_list = extract_normal_log_cluster(
            start_time=start_time,
            end_time=end_time,
            path=log_folder_path,
            lib=lib,
            sample_points=sample_points,
            llm_config=llm_config,
            input_file=input_file
        )
        
        if not workflow_list:
            logger.warning("未提取到任何任务流程")
            return None
        
        logger.info(f"成功提取 {len(workflow_list)} 个任务流程")
        
        # 获取当前最大ID，用于自增
        if self.df.empty or 'id' not in self.df.columns:
            current_id = 0
        else:
            # 确保 id 列是数值类型
            if self.df['id'].dtype == 'object':
                # 尝试转换为数值类型
                self.df['id'] = pd.to_numeric(self.df['id'], errors='coerce')
            current_id = int(self.df['id'].max()) if not self.df['id'].isna().all() else 0
        
        # 遍历每个任务流程，添加到 DataFrame
        for workflow in workflow_list:
            current_id += 1
            job = workflow.get('name', '')
            workchain = workflow.get('description', '')
            calling_seq = workflow.get('chain', '')
            
            # 提示用户输入 prompt_message
            logger.info(f"\n{'='*60}")
            logger.info(f"任务 {current_id}: {job}")
            logger.info(f"工作链描述: {workchain[:100]}..." if len(workchain) > 100 else f"工作链描述: {workchain}")
            logger.info(f"调用序列: {calling_seq}")
            logger.info(f"{'='*60}")
            
            prompt_message = input(f'请输入任务 "{job}" 的提示信息 (prompt_message): ')
            
            # 添加到 DataFrame
            new_row = {
                'id': current_id,
                'job': job,
                'workchain': workchain,
                'prompt_message': prompt_message,
                'calling_seq': calling_seq
            }
            
            # 追加新行到 DataFrame
            self.df = pd.concat([self.df, pd.DataFrame([new_row])], ignore_index=True)
            
            logger.info(f"已添加任务 {current_id}: {job}")
        
        logger.info(f"生成完成，共添加 {len(workflow_list)} 条记录")
    
    def save(self):
        """
        将当前 DataFrame 保存至 CSV 文件
        """
        try:
            # 确保列的顺序正确
            columns_order = ['id', 'job', 'workchain', 'prompt_message', 'calling_seq']
            # 只保留存在的列
            existing_columns = [col for col in columns_order if col in self.df.columns]
            # 添加其他可能存在的列
            other_columns = [col for col in self.df.columns if col not in columns_order]
            final_columns = existing_columns + other_columns
            
            self.df[final_columns].to_csv(self.save_path, index=False, encoding='utf-8')
            logger.info(f"成功保存到文件: {self.save_path}，共 {len(self.df)} 条记录")
        except Exception as e:
            logger.error(f"保存文件失败: {e}")
            raise


def _check_log_record_exists(generator: LogDescriptionFileGenerator, log_file_path: str, log_file_name: str) -> int:
    """
    检查日志记录是否已存在
    
    :param generator: LogDescriptionFileGenerator 实例
    :param log_file_path: 日志文件完整路径
    :param log_file_name: 日志文件名
    :return: 如果记录存在，返回 id 列的值（int），否则返回 -1
    """
    # 如果 DataFrame 为空，直接返回 -1
    if generator.df.empty:
        return -1
    
    # 检查是否存在相同的 file_name 或 path
    if 'file_name' in generator.df.columns:
        mask_name = generator.df['file_name'] == log_file_name
        if mask_name.any():
            # 获取匹配行的 index（如果 id 是 index）或 id 列的值（如果 id 是普通列）
            matched_row = generator.df[mask_name].iloc[0]
            if 'id' in generator.df.columns:
                # id 是普通列
                row_id = matched_row['id']
            else:
                # id 是 index（因为 set_index('id') 已经执行）
                row_id = matched_row.name
            logger.info(f"日志文件 {log_file_name} 的记录已存在（通过 file_name 匹配），id: {row_id}")
            return int(row_id)
    
    if 'path' in generator.df.columns:
        mask_path = generator.df['path'] == log_file_path
        if mask_path.any():
            # 获取匹配行的 index（如果 id 是 index）或 id 列的值（如果 id 是普通列）
            matched_row = generator.df[mask_path].iloc[0]
            if 'id' in generator.df.columns:
                # id 是普通列
                row_id = matched_row['id']
            else:
                # id 是 index（因为 set_index('id') 已经执行）
                row_id = matched_row.name
            logger.info(f"日志文件 {log_file_name} 的记录已存在（通过 path 匹配），id: {row_id}")
            return int(row_id)
    
    return -1


def generate_log_description_file(
    log_folder_path: str,
    log_description_file_path: str,
    llm_config: dict,
    platform_info: Dict[str, List[str]] = None
):
    """
    生成日志描述文件
    
    :param log_folder_path: 日志文件夹路径，包含所有需要处理的日志文件
    :param log_description_file_path: 日志描述文件保存路径（同时也是日志元数据文件路径）
    :param llm_config: 大模型配置
    :param platform_info: 平台信息字典，key为日志文件路径，value为[platform, sub_platform]列表
    :return: None
    """
    logger.info("="*60)
    logger.info("开始生成日志描述文件")
    logger.info("="*60)
    logger.info(f"日志文件夹路径: {log_folder_path}")
    logger.info(f"日志描述文件保存路径: {log_description_file_path}")
    
    if not os.path.isdir(log_folder_path):
        logger.error(f"日志文件夹路径不存在: {log_folder_path}")
        return None
    
    # 初始化日志描述文件生成器
    log_desc_generator = LogDescriptionFileGenerator(
        path=log_description_file_path,
        llm_config=llm_config
    )
    
    # 获取日志文件夹中的所有日志文件
    log_files = [f for f in os.listdir(log_folder_path) 
                 if os.path.isfile(os.path.join(log_folder_path, f))]
    
    if not log_files:
        logger.warning(f"日志文件夹 {log_folder_path} 中没有找到日志文件")
    else:
        logger.info(f"找到 {len(log_files)} 个日志文件，开始处理...")
        
        # 处理每个日志文件
        for idx, log_file in enumerate(log_files, 1):
            log_file_path = os.path.join(log_folder_path, log_file)
            logger.info(f"\n[{idx}/{len(log_files)}] 正在处理日志文件: {log_file}")
            
            try:
                # 检查记录是否已存在
                existing_row_index = _check_log_record_exists(
                    log_desc_generator, 
                    log_file_path, 
                    log_file
                )
                
                if existing_row_index != -1:
                    logger.info(f"日志文件 {log_file} 的记录已存在，跳过生成")
                    continue
                
                # 从 platform_info 中获取平台信息
                if platform_info and log_file_path in platform_info:
                    platform_data = platform_info[log_file_path]
                    current_platform = platform_data[0] if len(platform_data) > 0 else "unknown"
                    current_sub_platform = platform_data[1] if len(platform_data) > 1 else None
                else:
                    current_platform = "unknown"
                    current_sub_platform = None
                    logger.warning(f"日志文件 {log_file} 未在 platform_info 中找到平台信息，使用默认值")
                
                # 生成日志描述信息
                log_desc_generator.generate(
                    log_file_path=log_file_path,
                    platform=current_platform,
                    sub_platform=current_sub_platform,
                    log_metadata_row_name=-1  # -1 表示新增记录
                )
                logger.info(f"日志文件 {log_file} 处理完成")
            except Exception as e:
                logger.error(f"处理日志文件 {log_file} 时出错: {e}")
                import traceback
                traceback.print_exc()
                continue
        
        # 保存日志描述文件
        logger.info("\n保存日志描述文件...")
        log_desc_generator.save()
        logger.info(f"日志描述文件已保存到: {log_description_file_path}")
    
    logger.info("="*60)
    logger.info("日志描述文件生成完成！")
    logger.info("="*60)


def generate_cross_system_workflow_file(
    log_folder_path: str,
    log_description_file_path: str,
    cross_system_workflow_file_path: str,
    start_time: str,
    end_time: str,
    sample_points: int,
    llm_config: dict,
    input_file: List[str]
):
    """
    生成跨系统任务工作流文件
    
    :param log_folder_path: 日志文件夹路径，包含所有需要处理的日志文件
    :param log_description_file_path: 日志描述文件路径（作为日志元数据文件路径）
    :param cross_system_workflow_file_path: 跨系统任务工作流文件保存路径
    :param start_time: 开始时间，格式为 'YYYY-MM-DD HH:MM:SS'
    :param end_time: 结束时间，格式为 'YYYY-MM-DD HH:MM:SS'
    :param sample_points: 采样点数量（每个采样点对应一个3秒的时间窗口）
    :param llm_config: 大模型配置
    :param input_file: 入口日志文件列表，例如 ['https_access.log', 'ssh_access.log']
    :return: None
    """
    logger.info("="*60)
    logger.info("开始生成跨系统任务工作流文件")
    logger.info("="*60)
    
    if not os.path.isdir(log_folder_path):
        logger.error(f"日志文件夹路径不存在: {log_folder_path}")
        return None
    
    # 初始化跨系统任务工作流生成器
    workflow_generator = CrossSystemTaskWorkflowGenerator(
        save_path=cross_system_workflow_file_path
    )
    
    # 生成工作流
    logger.info("开始生成跨系统任务工作流...")
    workflow_generator.generate(
        log_folder_path=log_folder_path,
        start_time=start_time,
        end_time=end_time,
        lib=log_description_file_path,  # 使用日志描述文件路径作为日志元数据文件路径
        sample_points=sample_points,
        llm_config=llm_config,
        input_file=input_file
    )
    
    # 保存工作流文件
    logger.info("\n保存跨系统任务工作流文件...")
    workflow_generator.save()
    logger.info(f"跨系统任务工作流文件已保存到: {cross_system_workflow_file_path}")
    
    logger.info("="*60)
    logger.info("跨系统任务工作流文件生成完成！")
    logger.info("="*60)



def generate_knowledge_base(
    log_folder_path: str,
    log_description_file_path: str,
    cross_system_workflow_file_path: str,
    start_time: str,
    end_time: str,
    sample_points: int,
    llm_config: dict,
    input_file: List[str],
    log_path,
    platform_info: Dict[str, List[str]] = None,
):
    """
    综合调用 LogDescriptionFileGenerator 和 CrossSystemTaskWorkflowGenerator
    生成完整的知识库文件

    :param log_folder_path: 日志文件夹路径，包含所有需要处理的日志文件
    :param log_description_file_path: 日志描述文件保存路径（用于 LogDescriptionFileGenerator，同时也是日志元数据文件路径）
    :param cross_system_workflow_file_path: 跨系统任务工作流文件保存路径（用于 CrossSystemTaskWorkflowGenerator）
    :param start_time: 开始时间，格式为 'YYYY-MM-DD HH:MM:SS'
    :param end_time: 结束时间，格式为 'YYYY-MM-DD HH:MM:SS'
    :param sample_points: 采样点数量（每个采样点对应一个3秒的时间窗口）
    :param llm_config: 大模型配置
    :param input_file: 入口日志文件列表，例如 ['https_access.log', 'ssh_access.log']
    :param platform_info: 平台信息字典，key为日志文件路径，value为[platform, sub_platform]列表
    :return: None
    """
    logger.info("=" * 60)
    logger.info("开始生成知识库文件")
    logger.info("=" * 60)

    # 第一步: 处理日志文件夹中的每个日志文件，生成日志描述文件
    logger.info("\n第一步: 生成日志描述文件")
    logger.info(f"日志文件夹路径: {log_folder_path}")
    logger.info(f"日志描述文件保存路径: {log_description_file_path}")

    if not os.path.isdir(log_folder_path):
        logger.error(f"日志文件夹路径不存在: {log_folder_path}")
        return None

    # 初始化日志描述文件生成器
    log_desc_generator = LogDescriptionFileGenerator(
        path=log_description_file_path,
        llm_config=llm_config
    )

    # 获取日志文件夹中的所有日志文件
    log_files = [f for f in os.listdir(log_path)
                 if os.path.isfile(os.path.join(log_path, f))]

    if not log_files:
        logger.warning(f"日志文件夹 {log_path} 中没有找到日志文件")
    else:
        logger.info(f"找到 {len(log_files)} 个日志文件，开始处理...")

        # 处理每个日志文件
        for idx, log_file in enumerate(log_files, 1):
            log_file_path = os.path.join(log_path, log_file)
            logger.info(f"\n[{idx}/{len(log_files)}] 正在处理日志文件: {log_file}")

            # try:
            # 检查记录是否已存在
            existing_row_index = _check_log_record_exists(
                log_desc_generator,
                log_file_path,
                log_file
            )

            if existing_row_index != -1:
                logger.info(f"日志文件 {log_file} 的记录已存在，跳过生成")
                continue

            # 从 platform_info 中获取平台信息
            if platform_info and log_file_path in platform_info:
                platform_data = platform_info[log_file_path]
                current_platform = platform_data[0] if len(platform_data) > 0 else "unknown"
                current_sub_platform = platform_data[1] if len(platform_data) > 1 else None
            else:
                current_platform = "unknown"
                current_sub_platform = None
                logger.warning(f"日志文件 {log_file} 未在 platform_info 中找到平台信息，使用默认值")

            # 生成日志描述信息
            log_desc_generator.generate(
                log_file_path=log_file_path,
                platform=current_platform,
                sub_platform=current_sub_platform,
                log_metadata_row_name=-1  # -1 表示新增记录
            )
            logger.info(f"日志文件 {log_file} 处理完成")
            # except Exception as e:
            #     logger.error(f"处理日志文件 {log_file} 时出错: {e}")
            #     continue

        # 保存日志描述文件
        logger.info("\n保存日志描述文件...")
        log_desc_generator.save()
        logger.info(f"日志描述文件已保存到: {log_description_file_path}")

    # 第二步: 生成跨系统任务工作流文件
    logger.info("\n" + "=" * 60)
    logger.info("第二步: 生成跨系统任务工作流文件")
    logger.info("=" * 60)

    # 初始化跨系统任务工作流生成器
    workflow_generator = CrossSystemTaskWorkflowGenerator(
        save_path=cross_system_workflow_file_path
    )

    # 生成工作流
    logger.info("开始生成跨系统任务工作流...")
    workflow_generator.generate(
        log_folder_path=log_folder_path,
        start_time=start_time,
        end_time=end_time,
        lib=log_description_file_path,  # 使用日志描述文件路径作为日志元数据文件路径
        sample_points=sample_points,
        llm_config=llm_config,
        input_file=input_file
    )

    # 保存工作流文件
    logger.info("\n保存跨系统任务工作流文件...")
    workflow_generator.save()
    logger.info(f"跨系统任务工作流文件已保存到: {cross_system_workflow_file_path}")

    logger.info("\n" + "=" * 60)
    logger.info("知识库文件生成完成! ")
    logger.info("=" * 60)
    logger.info(f"日志描述文件: {log_description_file_path}")
    logger.info(f"跨系统工作流文件: {cross_system_workflow_file_path}")
    logger.info("=" * 60)


def preprocess_logs_with_regex_map(
    log_regex_map: Dict[str, str],
    output_folder: str
):
    """
    根据给定的 日志路径:正则 映射关系，对日志做多行合并预处理，并输出到指定文件夹。
    - log_regex_map: {log_path: prefix_regex}
    - output_folder: 预处理后日志的输出目录，目标文件名与原文件名相同
    """
    logger.info("=" * 60)
    logger.info("开始批量预处理日志文件（多行合并）")
    logger.info("=" * 60)

    if not os.path.isdir(output_folder):
        os.makedirs(output_folder, exist_ok=True)
        logger.info(f"创建输出目录: {output_folder}")

    for src_path, prefix_regex in log_regex_map.items():
        if not os.path.isfile(src_path):
            logger.warning(f"日志文件不存在，跳过: {src_path}")
            continue

        file_name = os.path.basename(src_path)
        dst_path = os.path.join(output_folder, file_name)

        logger.info(f"预处理日志文件: {src_path} -> {dst_path}")
        try:
            preprocess_log_file(src_path=src_path, dst_path=dst_path, prefix_regex=prefix_regex)
        except Exception as e:
            logger.error(f"预处理日志文件 {src_path} 时出错: {e}")
            import traceback
            traceback.print_exc()
            continue

    logger.info("=" * 60)
    logger.info("日志预处理完成！")
    logger.info("=" * 60)


if __name__ == '__main__':
    """
    测试 generate_knowledge_base 函数
    使用示例：
    1. 修改 log_folder_path 为实际的日志文件夹路径
    2. 修改 start_time 和 end_time 为实际的时间范围
    3. 修改 sample_points 为采样点数量
    4. 修改 input_file 为实际的入口日志文件列表
    5. 修改 platform_info 为实际的平台信息字典
    6. 根据需要修改其他参数
    """
    import os
    
    # 设置工作目录（根据实际情况修改）
    # os.chdir('E:\\Self_code\\PycharmProjects\\ZTE\\fault-analysis')
    
    # 初始化 PromptLoader
    from core.prompts.prompt_loader import PromptLoader
    PromptLoader.from_paths(['core/prompts'])
    
    # 配置参数
    log_folder_path = 'resource/log'  # 日志文件夹路径，包含所有需要处理的日志文件
    log_description_file_path = 'resource/log_description.csv'  # 日志描述文件保存路径（同时也是日志元数据文件路径）
    cross_system_workflow_file_path = 'resource/cross_system_task_workflow.csv'  # 跨系统任务工作流文件保存路径
    start_time = '2025-07-29 09:56:42'  # 开始时间
    end_time = '2025-07-29 09:56:55'  # 结束时间
    sample_points = 3  # 采样点数量
    
    llm_config = configs.LLM_CONFIG
    
    input_file = ['https_access.log', 'ssh_access.log']  # 入口日志文件列表
    
    # 平台信息字典，key为日志文件路径，value为[platform, sub_platform]列表
    platform_info = {
        os.path.join(log_folder_path, 'https_access.log'): ['gerrit', 'http'],
        os.path.join(log_folder_path, 'ssh_access.log'): ['gerrit', 'ssh']
    }  # 根据实际情况修改
    
    # 检查必要的文件是否存在
    if not os.path.isdir(log_folder_path):
        logger.error(f"日志文件夹不存在: {log_folder_path}")
        logger.info("请修改 log_folder_path 为实际的日志文件夹路径")
    else:
        # 调用函数生成知识库
        try:
            generate_knowledge_base(
                log_folder_path=log_folder_path,
                log_description_file_path=log_description_file_path,
                cross_system_workflow_file_path=cross_system_workflow_file_path,
                start_time=start_time,
                end_time=end_time,
                sample_points=sample_points,
                llm_config=llm_config,
                input_file=input_file,
                platform_info=platform_info
            )
            logger.info("\n测试完成！")
        except Exception as e:
            logger.error(f"测试过程中出现错误: {e}")
            import traceback
            traceback.print_exc()



