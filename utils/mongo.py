import os
import sys
from pathlib import Path
import logging

# 配置日志
logger = logging.getLogger(__name__)

add_path = str(Path(__file__).parent.parent)
sys.path.append(add_path)
os.chdir(add_path)

from configs.config import mongo_url
from pymongo import MongoClient
import sys
import argparse
import json

client = MongoClient(mongo_url)

def search_company_data(company,key_word):
    """
    统计指定company_name和norm_company的数据数量，并返回完整数据
    Args:
        company_name (str): 公司名称
        norm_company (str): 标准化公司名称
    Returns:
        int: 符合条件的数据数量
    """
    try:
        # 连接MongoDB
        db = client["OmniDataCrafter"]
        collection = db["company"]
        # 构建查询条件
        query = {
            # "company_name": {"$regex": company_name},
            # "norm_company": {"$regex": norm_company}
            key_word: company
        }
        # 统计符合条件的数据数量
        count = collection.count_documents(query)
        # 获取完整文档数据
        documents = list(collection.find(query))
        # 关闭连接
        # client.close()
        return count, documents
        
    except Exception as e:
        logger.error(f"查询过程中发生错误: {e}")
        return 0, []
    
def upsert_data(collection_name, source_id, new_data):
    """
    插入或更新数据到MongoDB。
    Args:
        collection_name (str): MongoDB集合名称
        source_id: 文档的唯一标识符（_id字段）
        new_data (dict): 要插入或更新的数据
    Returns:
        tuple: (operation_type, object_id) 
               operation_type: 'inserted' 或 'updated'
               object_id: 文档的ObjectId
    """
    try:
        db = client['OmniDataCrafter']  
        collection = db[collection_name]
        
        # 定义查询条件
        query = {'_id': source_id}
        
        # 执行upsert操作
        result = collection.update_one(query, {'$set': new_data}, upsert=True)
        
        # 判断操作类型并返回相应的object_id
        if result.upserted_id is not None:
            # 新插入的文档
            object_id = result.upserted_id
            logger.info(f"数据插入成功，ID: {object_id}")
            return 'inserted', object_id
        elif result.modified_count > 0:
            # 更新了现有文档
            object_id = source_id
            logger.info(f"数据更新成功，ID: {object_id}")
            return 'updated', object_id
        else:
            # 文档已存在但没有变化
            object_id = source_id
            logger.debug(f"文档已存在且无需更新，ID: {object_id}")
            return 'no_change', object_id
                          
    except Exception as e:
        logger.error(f"upsert操作出错: {e}")
        return 'error', None
   

def insert_data(collection_name, data):
    """
    插入数据到MongoDB集合
    Args:
        collection_name: 集合名称
        data: 要插入的数据，字典格式或字典列表格式
    """
    try:
        db = client['OmniDataCrafter']  
        collection = db[collection_name]
        # 判断data是否为列表
        if isinstance(data, list):
            if not data:  # 空列表检查
                logger.warning("数据列表为空，跳过插入")
                return
            # 批量插入
            result = collection.insert_many(data)
            if result.inserted_ids:
                logger.info(f"批量数据插入成功，插入了 {len(result.inserted_ids)} 条数据")
                logger.debug(f"插入的ID列表: {result.inserted_ids[:5]}{'...' if len(result.inserted_ids) > 5 else ''}")
            else:
                logger.error("批量数据插入失败")
        else:
            # 单个文档插入
            result = collection.insert_one(data)
            if result.inserted_id:
                logger.info(f"数据插入成功，ID: {result.inserted_id}")
            else:
                logger.error("数据插入失败")
    except Exception as e:
        logger.error(f"插入数据出错: {e}")
    finally:
        # 移除client.close()，因为这是全局连接，不应该在函数中关闭
        pass


# print(search_company_data("中煤能源","company_name"))