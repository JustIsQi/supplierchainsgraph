from pymongo import MongoClient
from utils.mysql_util import get_company_name,get_type_name
import requests,json
import os
from utils.data_prepare import content_to_kv,read_single_md_file,Markdown_header_splits,Markdown2Text_with_header,table_to_text,Markdown2Text
from models.model_infer import qwen_chat,gpt_chat,CompanyExtractionResult,gpt5_infer
from models.prompt import WIND_ANNO_PROMPT
from data_transfer import JSONToNebula
from configs.config import nebula_config
import pandas as pd
from utils.split_markdown_by_headers import split_by_headers
from pathlib import Path
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 创建线程锁，用于线程安全的操作
file_write_lock = threading.Lock()

def value_check(parsed_data):
    for key,value in parsed_data.items():
        for k,v in value.items():
            if v is None:
                parsed_data[key][k] = ''
    return parsed_data

def get_report_data(md_content):
    sections = split_by_headers(md_content)
    filter_contents = []
    for section in sections:
        header = section['title']
        content = section['content']
        data = {'query': "上市公司年报章节内容匹配"+header, 'texts':  report_labels}

        title_rerank_res = requests.post("http://10.100.0.1:7981/rerank", json=data)
        title_scores = json.loads(title_rerank_res.text)[0]
        title_index,title_score = title_scores['index'],title_scores['score']
        
        if title_score > 0.7:
            # if "<table" in content:
            #     content = table_to_text(content).strip()
            filter_contents.append(content)
    # response = gpt_chat(WIND_ANNO_PROMPT.format(contents='\n'.join(filter_contents))).replace('None','')
    response = qwen_chat(WIND_ANNO_PROMPT.format(contents='\n'.join(filter_contents))).replace('None','')
    parsed_data = json.loads(response)
    
    return parsed_data

def process_single_file(file_path):
    """处理单个文件的工作函数"""
    try:
        logger.info(f"开始处理文件: {file_path}")
        md_content = read_single_md_file(file_path)
        parsed_data = get_report_data(md_content)
        
        # 使用线程锁确保文件写入的线程安全
        output_file = f"responses/{file_path.stem}_qwen_thinking.json"
        with file_write_lock:
            # 确保responses目录存在
            os.makedirs("responses", exist_ok=True)
            with open(output_file, 'w', encoding='utf-8') as fp:
                fp.write(json.dumps(parsed_data, ensure_ascii=False, indent=2))
        
        logger.info(f"成功处理文件: {file_path}")
        return {"success": True, "file": file_path, "message": "处理成功"}
    
    except Exception as e:
        error_msg = f"处理文件 {file_path} 时出错: {str(e)}"
        logger.error(error_msg)
        return {"success": False, "file": file_path, "error": str(e)}

def main():
    """主函数，实现多线程并发处理"""
    # 读取报告标签
    
    root = Path('./wind_anno')  # 把这里换成你要遍历的目录
    all_files = [p for p in root.rglob('*') if p.is_file()]
    
    # 限制处理文件数量（可根据需要调整）
    files_to_process = all_files[100:]
    
    # 设置线程池大小（可根据系统性能调整）
    max_workers = min(5, len(files_to_process))  # 最多4个线程，或文件数量
    
    logger.info(f"开始处理 {len(files_to_process)} 个文件，使用 {max_workers} 个线程")
    
    # 使用ThreadPoolExecutor进行并发处理
    successful_count = 0
    failed_count = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        future_to_file = {executor.submit(process_single_file, file_path): file_path 
                         for file_path in files_to_process}
        
        # 使用tqdm显示进度条
        with tqdm(total=len(files_to_process), desc="处理文件") as pbar:
            for future in as_completed(future_to_file):
                result = future.result()
                if result["success"]:
                    successful_count += 1
                else:
                    failed_count += 1
                pbar.update(1)
    
    logger.info(f"处理完成！成功: {successful_count}, 失败: {failed_count}")

def test():
    file_path = "wind_anno/年度报告_windanno_90e0072a-0dc7-51f4-8a8b-55ce703a0bcc原文.md"
    md_content = read_single_md_file(file_path)
    parsed_data = get_report_data(md_content)
    
    # 使用线程锁确保文件写入的线程安全
    output_file = f"responses/年度报告_windanno_90e0072a-0dc7-51f4-8a8b-55ce703a0bcc原文_qwen_thinking.json"
   
    with open(output_file, 'w', encoding='utf-8') as fp:
        fp.write(json.dumps(parsed_data, ensure_ascii=False, indent=2))
    fp.close()
    

global report_labels
report_labels = pd.read_excel("data/report.xlsx", engine='openpyxl').fillna('').values.tolist()
report_labels = [item[0]+' '+item[1] for item in report_labels]

if __name__ == "__main__":
    # main()
    test()



    # inserter = JSONToNebula.JSONToNebulaInserter(nebula_config)
# inserter.run_insertion(parsed_data)

#  Qwen3-30B-A3B-Thinking-2507 在供应商、客户信息、主营构成的属性抽取上比GPT5好很多