#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Extend NebulaGraph Schema for JSON Data
为支持JSON数据中的额外字段扩展NebulaGraph Schema
"""

import logging
from nebula3.gclient.net import ConnectionPool
from nebula3.Config import Config
from typing import Dict

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SchemaExtender:
    def __init__(self, nebula_config: Dict, space_name: str = "supply_chain"):
        """
        初始化Schema扩展器
        
        Args:
            nebula_config: NebulaGraph数据库配置
            space_name: NebulaGraph space名称
        """
        self.nebula_config = nebula_config
        self.space_name = space_name
        self.nebula_pool = None
        self.nebula_session = None
        
    def connect_database(self):
        """连接NebulaGraph数据库"""
        try:
            # 连接NebulaGraph
            config = Config()
            config.max_connection_pool_size = 10
            self.nebula_pool = ConnectionPool()
            self.nebula_pool.init([(self.nebula_config['host'], self.nebula_config['port'])], config)
            
            # 获取session
            session = self.nebula_pool.get_session(
                self.nebula_config['user'], 
                self.nebula_config['password']
            )
            self.nebula_session = session
            
            # 使用space
            use_space_query = f"USE {self.space_name}"
            result = self.nebula_session.execute(use_space_query)
            if not result.is_succeeded():
                logger.error(f"使用space失败: {result.error_msg()}")
                raise Exception(f"使用space失败: {result.error_msg()}")
            
            logger.info("NebulaGraph连接成功")
            
        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
            raise
    
    def close_connection(self):
        """关闭数据库连接"""
        if self.nebula_session:
            self.nebula_session.release()
        if self.nebula_pool:
            self.nebula_pool.close()
    
    def execute_query(self, query: str, description: str = ""):
        """执行查询"""
        try:
            result = self.nebula_session.execute(query)
            if not result.is_succeeded():
                logger.warning(f"{description}失败: {result.error_msg()}")
                return False
            logger.info(f"{description}成功")
            return True
        except Exception as e:
            logger.error(f"{description}异常: {e}")
            return False
    
    def extend_schema(self):
        """扩展Schema以支持JSON数据"""
        
        # 扩展边类型以支持额外的业务字段
        edge_extensions = [
            # 扩展SUPPLIES_TO边，添加供应内容和主要供应商标识
            """ALTER EDGE SUPPLIES_TO ADD (supply_content string, is_major_supplier bool)""",
            
            # 扩展CUSTOMER_OF边，添加业务内容和主要客户标识
            """ALTER EDGE CUSTOMER_OF ADD (business_content string, is_major_customer bool)""",
            
            # 扩展CONTROL_STAKE边，确保字段名称正确
            """ALTER EDGE CONTROL_STAKE ADD (shareholder_percentage string, shareholder_amount string, shareholder_value string)""",
        ]
        
        # 创建新的边类型（如果不存在）
        new_edges = [
            # 创建产品关系边 - 公司生产产品
            """CREATE EDGE IF NOT EXISTS PRODUCES (
                product_type string,
                business_type string,
                revenue string,
                revenue_percentage string,
                gross_profit_margin string,
                cost string,
                gross_profit string,
                currency string,
                report_period string,
                business_description string
            )""",
            
            # 创建合作关系边 - 用于关联公司
            """CREATE EDGE IF NOT EXISTS COOPERATES_WITH (
                cooperation_type string,
                relationship string,
                relationship_percentage string,
                business_scope string
            )""",
        ]
        
        # 扩展Company Tag以支持更多字段
        company_extensions = [
            """ALTER TAG Company ADD (
                registration_place string,
                business_place string, 
                industry string,
                business_scope string,
                company_qualification string
            )""",
        ]
        
        # 执行扩展
        logger.info("开始扩展Schema...")
        
        # 扩展现有标签
        for query in company_extensions:
            self.execute_query(query, "扩展Company标签")
        
        # 创建新边类型
        for query in new_edges:
            self.execute_query(query, "创建新边类型")
            
        # 扩展现有边类型（可能失败，如果字段已存在）
        for query in edge_extensions:
            self.execute_query(query, "扩展边类型")
        
        # 等待schema同步
        import time
        time.sleep(5)
        
        logger.info("Schema扩展完成")
    
    def run_extension(self):
        """运行Schema扩展"""
        try:
            logger.info("开始Schema扩展...")
            
            # 连接数据库
            self.connect_database()
            
            # 扩展Schema
            self.extend_schema()
            
            logger.info("Schema扩展完成!")
            
        except Exception as e:
            logger.error(f"Schema扩展失败: {e}")
            raise
        finally:
            self.close_connection()

# 使用示例
if __name__ == "__main__":
    # NebulaGraph配置
    nebula_config = {
        'host': '10.100.0.205',
        'port': 9669,
        'user': 'root',
        'password': 'nebula'
    }
    
    # 创建扩展器
    extender = SchemaExtender(nebula_config)
    
    # 执行扩展
    extender.run_extension() 