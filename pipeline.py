from pymongo import MongoClient
from utils.mysql_util import get_company_name,get_type_name
import requests,json
import os
from utils.data_prepare import content_to_kv,read_single_md_file,Markdown_header_splits,Markdown2Text_with_header,table_to_text,Markdown2Text
from models.model_infer import qwen_chat
from models.prompt import WIND_ANNO_PROMPT
from data_transfer.JSONToNebula import JSONToNebulaInserter
from configs.config import *
import pandas as pd
from utils.split_markdown_by_headers import split_by_headers
from utils.redis_cache import fetch_one,ack,rollback_unprocessed,failed_rollback
from utils.use_tool import US3Client
from pathlib import Path
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import logging
import time

# 配置日志
def setup_logger():
    """设置日志记录"""
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    log_file_path = f'/data/share2/yy/workspace/logs/wind_anno_qwen_inference.log'
    
    # 创建日志目录
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
    
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.INFO)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    return logger

logger = setup_logger()

global report_labels
report_labels = pd.read_excel("data/report.xlsx", engine='openpyxl').fillna('').values.tolist()
report_labels = [item[0]+' '+item[1] for item in report_labels]
us3_client = US3Client()
inserter = JSONToNebulaInserter(nebula_config,space_name="YXSupplyChains")

def value_check(parsed_data):
    for key,value in parsed_data.items():
        for k,v in value.items():
            if v is None:
                parsed_data[key][k] = ''
    return parsed_data

def qwen_inference_pipeline(md_content):
    sections = split_by_headers(md_content)
    filter_contents = []
    for section in sections:
        header = section['title']
        content = section['content']

        data = {"model": "Bge-ReRanker",'query': "上市公司年报章节内容匹配"+header, 'documents':report_labels}
        # print("raw docs:", paras,'\n\n')

        headers = {
            "accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer sk-1234",
        }
        r = requests.post('http://10.100.0.205:4000/rerank',headers=headers, json=data)
        ranked_results = json.loads(r.text)['results']

        original_order_scores = []
        for result in ranked_results:
            original_index,original_score = result["index"],result["relevance_score"]
            original_order_scores.append({"index":original_index,"score":original_score})

        # data = {'query': "上市公司年报章节内容匹配"+header, 'texts':  report_labels}
        # title_rerank_res = requests.post("http://10.100.0.1:7981/rerank", json=data)
        # title_scores = json.loads(title_rerank_res.text)[0]
        title_scores = original_order_scores[0]
        title_index,title_score = title_scores['index'],title_scores['score']
        
        if title_score > 0.7:
            # if "<table" in content:
            #     content = table_to_text(content).strip()
            filter_contents.append(content)
    response = qwen_chat(WIND_ANNO_PROMPT.format(contents='\n'.join(filter_contents))).replace('None','')
    parsed_data = json.loads(response)
    return parsed_data

def process_single_file(file_path):
    """处理单个文件的工作函数"""
    try:
        logger.info(f"开始处理文件: {file_path}")
        md_content = read_single_md_file(file_path)
        parsed_data = qwen_inference_pipeline(md_content)
        # 使用线程锁确保文件写入的线程安全
        filename = Path(file_path).stem
        output_file = f"{QWEN_INFERENCE_JSON_PATH}/{filename}{QWEN_FILE_SUFFIX}"
    
        # 确保responses目录存在
        os.makedirs(QWEN_INFERENCE_JSON_PATH, exist_ok=True)
        with open(output_file, 'w', encoding='utf-8') as fp:
            fp.write(json.dumps(parsed_data, ensure_ascii=False, indent=2))
        fp.close()
        
        inserter.run_insertion(parsed_data)
        logger.info(f"成功处理文件: {file_path}")
        return {"success": True, "file": file_path, "message": "处理成功"}
    
    except Exception as e:
        error_msg = f"处理文件 {file_path} 时出错: {e.message}"
        logger.error(error_msg)
        return {"success": False, "file": file_path, "error": e.message}

    
if __name__ == "__main__":
    rollback_unprocessed()
    while True:
        data = fetch_one()
        if not data:
            logger.info("队列为空，等待新任务...")
            time.sleep(5)  # 等待5秒后再检查
            continue
        use_path = data["use_path"]
        us3_file_name = use_path.split("/")[-1]
        tmp_path = os.path.join(QWEN_INFERENCE_MD_PATH, us3_file_name)
        print(us3_client.download_file(use_path, tmp_path))

        res = process_single_file(tmp_path)
        if res["success"]:
            ack(data["id"])
        else:
            failed_rollback(data["id"])


