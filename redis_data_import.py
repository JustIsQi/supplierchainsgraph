import requests,json
import numpy as np
import pandas as pd
import re,os,logging
from pymongo import MongoClient
from configs.config import *
import threading
import redis
from datetime import datetime
from utils.mysql_util import get_type_name
from utils.redis_cache import insert_dicts

def setup_logger():
    """设置日志记录"""
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    log_file_path = f'/data/share2/yy/workspace/logs/redis_data_import.log'
    
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

class MongoInference:
    def __init__(self, mongo_url, skip=0):
        self.logger = logger
        self.client = MongoClient(mongo_url)
        self.skip = skip
        self.processed_count = 0  # 已处理数据计数器
        self.redis_conn = redis.Redis(
            host=redis_config["host"],
            port=redis_config["port"],
            password=redis_config["password"],  # 如果 Redis 有密码，需要填写
            db=redis_config["db"],  # Redis数据库编号必须是整数，默认是0
            socket_timeout=5,
            socket_connect_timeout=5,
            decode_responses=True  # 自动解码响应，避免bytes类型问题
        )
       

    def batch_documents_sample(self,collection_name, batch_size=40):
        """org-39ef2751310848e4a4aa2066a8bcbd21
        分批循环获取并打印MongoDB集合中的所有文档
        Args:
            collection_name: 集合名称
            batch_size: 每批获取的文档数量，默认50条
        """
        self.logger.info(f"=== {collection_name} 集合中的文档（分批获取，每批{batch_size}条） ===")
        db = self.client['OmniDataCrafter']  
        collection = db[collection_name]
        total_count = collection.count_documents({})
        self.logger.info(f"总共找到 {total_count} 个文档")
        processed_count = 0
        
        batch_num = 1
        while self.skip < total_count: # 1755810
            self.logger.info(f"\n=== 第 {batch_num} 批（跳过 {self.skip} 条，获取 {batch_size} 条） ===")
            cursor = collection.find({}).skip(self.skip).limit(batch_size)
            batch_count,batch_contents = 0,[]
            for doc in cursor:
                batch_count += 1
                # # 处理批内数据、
                source_id = doc.get('_id',"")
                n_info_fcode = doc.get('n_info_fcode',"")
                tag = get_type_name(n_info_fcode)
                
                if tag not in ["半年报告","年度报告"]:
                    continue
                
                news = doc.get('rt_parser',"")
                if isinstance(news,str):
                    continue
                if len(list(news.keys())) == 0:
                    continue
                logger.info(f"开始处理 {source_id} {tag}")
                news_id = list(news.keys())[0]
                use_path = news[news_id]['us3_path']

                batch_contents.append({
                    "id": source_id,
                    "use_path": use_path
                })

                self.processed_count += 1
            if len(batch_contents) > 0: 
                insert_dicts(batch_contents)
                        
            self.skip += batch_size
            batch_num += 1

# 创建MongoInference实例，设置每5000条数据统计Top10公司
mongo_inference = MongoInference(mongo_url, skip=0)
# 使用原有函数处理历史数据（可选）
mongo_inference.batch_documents_sample("wind_announcement")
