#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MySQL to NebulaGraph Converter for Supply Chain Management System
根据用户手册中的表结构严格转换数据
"""

import mysql.connector
from nebula3.gclient.net import ConnectionPool
from nebula3.Config import Config
import pandas as pd
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime
from tqdm import tqdm
from collections import Counter
from pypinyin import lazy_pinyin, Style
import json

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def is_pinyin(chinese_str, input_pinyin, style=Style.NORMAL):
    """
    Check if the input_pinyin matches the pinyin of the chinese_str.
    """
    pinyin_list = lazy_pinyin(chinese_str, style=style)
    pinyin_str = ''.join(pinyin_list)
    return pinyin_str.lower() == input_pinyin.lower()

def is_valid_data(r_orig, t_orig) -> bool:
    if not r_orig or not t_orig:
        return False

    r_orig = r_orig.split('及子')[0]
    r_orig = r_orig.split('及其')[0]
    t_orig = t_orig.split('及子')[0]
    t_orig = t_orig.split('及其')[0]

    if r_orig in t_orig or t_orig in r_orig:
        return False

    if r_orig == t_orig:
        return False

    c_r_orig = Counter(r_orig)
    c_t_orig = Counter(t_orig)

    if c_r_orig <= c_t_orig or c_t_orig <= c_r_orig:
        return False

    r_orig = r_orig.replace("有限责任公司", "有限公司")
    r_orig = r_orig.replace("股份有限公司", "有限公司")
    r_orig = r_orig.replace("集团有限公司", "有限公司")
    r_orig = r_orig.replace("科技股份有限公司", "有限公司")
    r_orig = r_orig.replace("投资有限公司", "有限公司")
    r_orig = r_orig.replace("控股有限公司", "有限公司")
    r_orig = r_orig.replace("投资股份有限公司", "有限公司")
    r_orig = r_orig.replace("有限公司", "公司")
    r_orig = r_orig.replace("公司", "")
    r_orig = r_orig.split('及子')[0]
    r_orig = r_orig.split('及其')[0]

    t_orig = t_orig.replace("有限责任公司", "有限公司")
    t_orig = t_orig.replace("股份有限公司", "有限公司")
    t_orig = t_orig.replace("集团有限公司", "有限公司")
    t_orig = t_orig.replace("科技股份有限公司", "有限公司")
    t_orig = t_orig.replace("投资有限公司", "有限公司")
    t_orig = t_orig.replace("控股有限公司", "有限公司")
    t_orig = t_orig.replace("投资股份有限公司", "有限公司")
    t_orig = t_orig.replace("有限公司", "公司")
    t_orig = t_orig.replace("公司", "")
    t_orig = t_orig.split('及子')[0]
    t_orig = t_orig.split('及其')[0]

    r_orig = r_orig.strip().replace(" ", "").lower()
    t_orig = t_orig.strip().replace(" ", "").lower()

    r_orig = "".join(r_orig.split('、'))
    r_orig = "".join(r_orig.split('，'))
    r_orig = "".join(r_orig.split('；'))
    r_orig = "".join(r_orig.split(','))
    r_orig = "".join(r_orig.split('.'))
    
    t_orig = "".join(t_orig.split('、'))
    t_orig = "".join(t_orig.split('，'))
    t_orig = "".join(t_orig.split('；'))
    t_orig = "".join(t_orig.split(','))
    t_orig = "".join(t_orig.split('.'))

    if r_orig in t_orig or t_orig in r_orig:
        return False

    if r_orig == t_orig:
        return False

    if is_pinyin(r_orig, t_orig) or is_pinyin(t_orig, r_orig):
        return False

    c_r_orig = Counter(r_orig)
    c_t_orig = Counter(t_orig)

    if c_r_orig <= c_t_orig or c_t_orig <= c_r_orig:
        return False
    
    return True

def escape_string_for_nebula(value):
    """转义字符串用于NebulaGraph查询"""
    if value is None:
        return "NULL"
    if isinstance(value, str):
        # 转义特殊字符
        value = value.replace('\\', '\\\\')
        value = value.replace('"', '\\"')
        value = value.replace('\n', '\\n')
        value = value.replace('\r', '\\r')
        value = value.replace('\t', '\\t')
        return f'"{value}"'
    elif isinstance(value, (int, float)):
        return str(value) if value is not None else "NULL"
    elif isinstance(value, bool):
        return "true" if value else "false"
    else:
        return f'"{str(value)}"'

class MySQLToNebulaConverter:
    def __init__(self, mysql_config: Dict, nebula_config: Dict, space_name: str = "SupplyChainsDev"):
        """
        初始化转换器
        
        Args:
            mysql_config: MySQL数据库配置
            nebula_config: NebulaGraph数据库配置
            space_name: NebulaGraph space名称
        """
        self.mysql_config = mysql_config
        self.nebula_config = nebula_config
        self.space_name = space_name
        self.mysql_conn = None
        self.nebula_pool = None
        self.nebula_session = None
        self.batch_size = 1000
        
    def connect_databases(self):
        """连接MySQL和NebulaGraph数据库"""
        try:
            # 连接MySQL
            self.mysql_conn = mysql.connector.connect(**self.mysql_config)
            logger.info("MySQL连接成功")
            
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
            
            logger.info("NebulaGraph连接成功")
            
        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
            raise
    
    def close_connections(self):
        """关闭数据库连接"""
        if self.mysql_conn:
            self.mysql_conn.close()
        if self.nebula_session:
            self.nebula_session.release()
        if self.nebula_pool:
            self.nebula_pool.close()
    
    def create_space_and_schema(self):
        """创建NebulaGraph space和schema"""
        try:
            # 创建space
            create_space_query = f"""
            CREATE SPACE IF NOT EXISTS {self.space_name} (
                partition_num = 10, 
                replica_factor = 1, 
                vid_type = FIXED_STRING(256)
            );USE {self.space_name}
            """
            result = self.nebula_session.execute(create_space_query)
            if not result.is_succeeded():
                logger.error(f"创建space失败: {result.error_msg()}")
                raise Exception(f"创建space失败: {result.error_msg()}")
            
            # 使用space
            use_space_query = f"USE {self.space_name}"
            result = self.nebula_session.execute(use_space_query)
            if not result.is_succeeded():
                logger.error(f"使用space失败: {result.error_msg()}")
                raise Exception(f"使用space失败: {result.error_msg()}")
            
            logger.info(f"Space {self.space_name} 创建/使用成功")
            
            # 等待schema同步
            import time
            time.sleep(10)
            
            # 创建Tags (相当于Neo4j的节点标签)
            tag_queries = [
                """CREATE TAG IF NOT EXISTS Company (
                    company_name string NOT NULL,
                    std_en string,
                    cdtid string,
                    csfid string,
                    orgid string,
                    is_bond_issuer string,
                    is_listing string
                )""",
                
                """CREATE TAG IF NOT EXISTS Person (
                    person_name string NOT NULL,
                    name_en string,
                    birth string,
                    ce_cd string,
                    ce_sch string,
                    ce_en string,
                    profq_code string,
                    profq_sch string,
                    profq_en string,
                    sex_sch string,
                    sex_en string,
                    til_sch string,
                    til_en string,
                    tilcd string
                )""",
                
                """CREATE TAG IF NOT EXISTS Stock (
                    stock_code string NOT NULL,
                    org_en string,
                    org string,
                    abbr string,
                    abbr_en string,
                    abbr_py string,
                    mkt_code string,
                    mkt_en string,
                    mkt string,
                    list_status string,
                    list_dt string,
                    list_edt string
                )""",
                
                """CREATE TAG IF NOT EXISTS Report (
                    businessid string NOT NULL,
                    rpt string,
                    fp string,
                    q string,
                    fy string,
                    p string,
                    publish_date string,
                    report_type string
                )"""
            ]
            
            # 创建Edge Types (相当于Neo4j的关系类型)
            edge_queries = [
                "CREATE EDGE IF NOT EXISTS ISSUES_STOCK ()",
                "CREATE EDGE IF NOT EXISTS PUBLISHES_REPORT ()",
                "CREATE EDGE IF NOT EXISTS HAS_REPORT ()",
                
                """CREATE EDGE IF NOT EXISTS PARENT_OF (
                    ticker string,
                    rpt string,
                    reg_std string,
                    unit string,
                    currency string,
                    capital double,
                    ratio double,
                    vote_ratio double
                )""",
                
                """CREATE EDGE IF NOT EXISTS SUBSIDIARY_OF (
                    ticker string,
                    rpt string,
                    reg_std string,
                    bizzplace_std string,
                    directrate double,
                    indirectrate double,
                    totalrate double
                )""",
                
                """CREATE EDGE IF NOT EXISTS NON_WHOLLY_SUBSIDIARY_OF (
                    ticker string,
                    rpt string,
                    ratio double,
                    unit string,
                    currency string,
                    gains double,
                    dividend double,
                    equity double
                )""",
                
                """CREATE EDGE IF NOT EXISTS JOINT_VENTURE_OF (
                    ticker string,
                    rpt string,
                    reg_sch string,
                    bizzplace_sch string,
                    directrate double,
                    indirectrate double,
                    totalrate double
                )""",
                
                """CREATE EDGE IF NOT EXISTS OTHER_RELATED_TO (
                    ticker string,
                    rpt string,
                    relation string
                )""",
                
                """CREATE EDGE IF NOT EXISTS CUSTOMER_OF (
                    ticker string,
                    rpt string,
                    cy_sch string,
                    cy_en string,
                    unit_sch string,
                    unit_en string,
                    amount double,
                    rate double,
                    typ string,
                    age string
                )""",
                
                """CREATE EDGE IF NOT EXISTS SUPPLIES_TO (
                    ticker string,
                    rpt string,
                    cy_sch string,
                    cy_en string,
                    unit_sch string,
                    unit_en string,
                    amount double,
                    rate double,
                    typ string,
                    age string
                )""",
                
                """CREATE EDGE IF NOT EXISTS RELATED_SALE (
                    ticker string,
                    rpt string,
                    cy_orig string,
                    cy_code string,
                    cy_unit string,
                    content string,
                    amount_curr double,
                    rate_curr double,
                    amount_prev double,
                    rate_prev double
                )""",
                
                """CREATE EDGE IF NOT EXISTS RELATED_PURCHASE (
                    ticker string,
                    rpt string,
                    cy_orig string,
                    cy_code string,
                    cy_unit string,
                    content string,
                    amount_curr double,
                    rate_curr double,
                    amount_prev double,
                    rate_prev double,
                    amount_limit double,
                    exceed string
                )""",
                
                """CREATE EDGE IF NOT EXISTS OTHER_PAYMENT_TO (
                    ticker string,
                    rpt string,
                    cy_orig string,
                    cy_code string,
                    cy_unit string,
                    amount double,
                    rate double
                )""",
                
                """CREATE EDGE IF NOT EXISTS BAD_DEBT_TO (
                    ticker string,
                    rpt string,
                    cy_orig string,
                    cy_code string,
                    cy_unit string,
                    typ string,
                    amount double,
                    ratio double,
                    debt double,
                    description string
                )""",
                
                """CREATE EDGE IF NOT EXISTS OTHER_RECEIVABLE_FROM (
                    ticker string,
                    rpt string,
                    cy_orig string,
                    cy_code string,
                    cy_unit string,
                    amount double,
                    ratio double,
                    dt string,
                    dc string
                )""",
                
                """CREATE EDGE IF NOT EXISTS RELATED_AR_FROM (
                    ticker string,
                    rpt string,
                    items_orig string,
                    items_sch string,
                    items_en string,
                    items_code string,
                    cy_orig string,
                    cy_code string,
                    cy_unit string,
                    amount_curr double,
                    rate_curr double,
                    amount_prev double,
                    rate_prev double,
                    debt_curr double,
                    debt_prev double
                )""",
                
                """CREATE EDGE IF NOT EXISTS RELATED_AP_TO (
                    ticker string,
                    rpt string,
                    items_orig string,
                    items_sch string,
                    items_en string,
                    items_code string,
                    cy_orig string,
                    cy_code string,
                    cy_unit string,
                    amount_curr double,
                    rate_curr double,
                    amount_prev double,
                    rate_prev double
                )"""
            ]
            
            # 执行创建Tag的查询
            for query in tag_queries:
                result = self.nebula_session.execute(query)
                if not result.is_succeeded():
                    logger.warning(f"创建Tag失败: {result.error_msg()}")
                else:
                    logger.info(f"创建Tag成功: {query.split()[4]}")
            
            # 等待schema同步
            time.sleep(5)
            
            # 执行创建Edge的查询
            for query in edge_queries:
                result = self.nebula_session.execute(query)
                if not result.is_succeeded():
                    logger.warning(f"创建Edge失败: {result.error_msg()}")
                else:
                    logger.info(f"创建Edge成功: {query.split()[4]}")
            
            # 等待schema同步
            time.sleep(5)
            
            # 创建索引
            index_queries = [
                "CREATE TAG INDEX IF NOT EXISTS company_name_index ON Company(company_name(256))",
                "CREATE TAG INDEX IF NOT EXISTS person_name_index ON Person(person_name(256))",
                "CREATE TAG INDEX IF NOT EXISTS stock_code_index ON Stock(stock_code(64))",
                "CREATE TAG INDEX IF NOT EXISTS businessid_index ON Report(businessid(64))"
            ]
            
            for query in index_queries:
                result = self.nebula_session.execute(query)
                if not result.is_succeeded():
                    logger.warning(f"创建索引失败: {result.error_msg()}")
                else:
                    logger.info(f"创建索引成功")
            
            # 等待索引生效
            time.sleep(10)
            
            logger.info("Schema创建完成")
            
        except Exception as e:
            logger.error(f"创建schema失败: {e}")
            raise

    def calculate_rank_from_date(self,date_str: str) -> int:
        """
        根据报告截止日期计算rank值
        日期越新，rank值越小（优先级越高）
        
        Args:
            date_str: 日期字符串，支持多种格式：
                    - "2024-12-31"
                    - "2024/12/31" 
                    - "20241231"
                    - "2024年12月31日"
        
        Returns:
            int: rank值，基于日期距离2000-01-01的天数取负值
        """
        if not date_str:
            return 0  # 如果没有日期，给最低优先级
        
        try:
            # 清理日期字符串
            date_clean = str(date_str).strip()
            
            # 移除双引号
            date_clean = date_clean.replace('"', '')
            
            # 移除中文字符
            import re
            date_clean = re.sub(r'[年月日]', '', date_clean)
            
            # 尝试多种日期格式
            date_formats = [
                '%Y-%m-%d',    # 2024-12-31
                '%Y/%m/%d',    # 2024/12/31
                '%Y%m%d',      # 20241231
                '%Y-%m',       # 2024-12 (如果只有年月)
                '%Y/%m',       # 2024/12
                '%Y'           # 2024 (如果只有年)
            ]
            
            parsed_date = None
            for fmt in date_formats:
                try:
                    if fmt == '%Y-%m' or fmt == '%Y/%m':
                        # 如果只有年月，补充日为01
                        parsed_date = datetime.strptime(date_clean + '-01', '%Y-%m-%d')
                    elif fmt == '%Y':
                        # 如果只有年，补充为01-01
                        parsed_date = datetime.strptime(date_clean + '-01-01', '%Y-%m-%d')
                    else:
                        parsed_date = datetime.strptime(date_clean, fmt)
                    break
                except ValueError:
                    continue
                
            
            if parsed_date is None:
                logger.warning(f"无法解析日期格式: {date_str}")
                return 0
            
            # 计算距离2000-01-01的天数
            base_date = datetime(2000, 1, 1)
            days_diff = (parsed_date - base_date).days
            
            # 返回负值使得日期越新rank越大（优先级越高）
            return days_diff
            
        except Exception as e:
            logger.warning(f"计算日期rank时发生错误: {e}, 日期: {date_str}")
            return 0
    
    def clear_space(self):
        """清空space中的所有数据"""
        try:
            # 使用space
            use_space_query = f"USE {self.space_name}"
            result = self.nebula_session.execute(use_space_query)
            if not result.is_succeeded():
                logger.error(f"使用space失败: {result.error_msg()}")
                return
            
            # 删除所有边
            result = self.nebula_session.execute("MATCH ()-[e]->() DELETE e")
            if result.is_succeeded():
                logger.info("删除所有边成功")
            
            # 删除所有顶点
            result = self.nebula_session.execute("MATCH (v) DELETE v")
            if result.is_succeeded():
                logger.info("删除所有顶点成功")
                
        except Exception as e:
            logger.warning(f"清空space失败: {e}")
    
    def _execute_batch_insert(self, query: str, description: str = ""):
        """执行批量插入查询"""
        try:
            result = self.nebula_session.execute(query)
            if not result.is_succeeded():
                logger.error(f"{description}失败: {result.error_msg()}")
                logger.error(f"查询: {query[:500]}...")
                return False
            return True
        except Exception as e:
            logger.error(f"{description}异常: {e}")
            logger.error(f"查询: {query[:500]}...")
            return False
    
    def migrate_std_org(self):
        """迁移企业信息表 - 创建Company顶点"""
        query = """
        SELECT id, csfid, std_sch, std_en, cdtid, orgid
        FROM std_org
        """
        
        df = pd.read_sql(query, self.mysql_conn)
        total_records = len(df)
        data_batches = [df[i:i + self.batch_size] for i in range(0, total_records, self.batch_size)]
        
        for idx, batch_df in enumerate(tqdm(data_batches, total=len(data_batches), desc="迁移 std_org 数据")):
            values = []
            for _, row in batch_df.iterrows():
                # 使用公司名称作为VID
                vid = escape_string_for_nebula(row['std_sch'])
                company_name = escape_string_for_nebula(row['std_sch'])
                std_en = escape_string_for_nebula(row['std_en'] if pd.notna(row['std_en']) else '')
                cdtid = escape_string_for_nebula(row['cdtid'] if pd.notna(row['cdtid']) else '')
                csfid = escape_string_for_nebula(row['csfid'] if pd.notna(row['csfid']) else '')
                orgid = escape_string_for_nebula(row['orgid'] if pd.notna(row['orgid']) else '')
                
                values.append(f"{vid}: ({company_name}, {std_en}, {cdtid}, {csfid}, {orgid}, \"\", \"\")")
            
            if values:
                insert_query = f"""
                INSERT VERTEX Company(company_name, std_en, cdtid, csfid, orgid, is_bond_issuer, is_listing) VALUES
                {', '.join(values)}
                """
                
                self._execute_batch_insert(insert_query, f"迁移 std_org 批次 {idx + 1}/{len(data_batches)}")
        
        logger.info(f"迁移完成 std_org: {len(df)} 条记录")
    
    def migrate_new_base_people(self):
        """迁移人物信息表 - 创建Person顶点"""
        query = """
        SELECT id, name_sch, name_en, birth, ce_cd, ce_sch, ce_en,
               profq_code, profq_sch, profq_en, sex_sch, sex_en, til_sch, til_en, tilcd
        FROM new_base_people
        """
        
        df = pd.read_sql(query, self.mysql_conn)
        data_batches = [df[i:i + self.batch_size] for i in range(0, len(df), self.batch_size)]
        
        for idx, batch_df in enumerate(tqdm(data_batches, total=len(data_batches), desc="迁移 new_base_people 数据")):
            values = []
            for _, row in batch_df.iterrows():
                vid = escape_string_for_nebula(str(row['id']))
                person_name = escape_string_for_nebula(row['name_sch'] if pd.notna(row['name_sch']) else '')
                name_en = escape_string_for_nebula(row['name_en'] if pd.notna(row['name_en']) else '')
                birth = escape_string_for_nebula(str(row['birth']) if pd.notna(row['birth']) else '')
                ce_cd = escape_string_for_nebula(row['ce_cd'] if pd.notna(row['ce_cd']) else '')
                ce_sch = escape_string_for_nebula(row['ce_sch'] if pd.notna(row['ce_sch']) else '')
                ce_en = escape_string_for_nebula(row['ce_en'] if pd.notna(row['ce_en']) else '')
                profq_code = escape_string_for_nebula(row['profq_code'] if pd.notna(row['profq_code']) else '')
                profq_sch = escape_string_for_nebula(row['profq_sch'] if pd.notna(row['profq_sch']) else '')
                profq_en = escape_string_for_nebula(row['profq_en'] if pd.notna(row['profq_en']) else '')
                sex_sch = escape_string_for_nebula(row['sex_sch'] if pd.notna(row['sex_sch']) else '')
                sex_en = escape_string_for_nebula(row['sex_en'] if pd.notna(row['sex_en']) else '')
                til_sch = escape_string_for_nebula(row['til_sch'] if pd.notna(row['til_sch']) else '')
                til_en = escape_string_for_nebula(row['til_en'] if pd.notna(row['til_en']) else '')
                tilcd = escape_string_for_nebula(row['tilcd'] if pd.notna(row['tilcd']) else '')
                
                values.append(f"{vid}: ({person_name}, {name_en}, {birth}, {ce_cd}, {ce_sch}, {ce_en}, {profq_code}, {profq_sch}, {profq_en}, {sex_sch}, {sex_en}, {til_sch}, {til_en}, {tilcd})")
            
            if values:
                insert_query = f"""
                INSERT VERTEX Person(person_name, name_en, birth, ce_cd, ce_sch, ce_en, profq_code, profq_sch, profq_en, sex_sch, sex_en, til_sch, til_en, tilcd) VALUES
                {', '.join(values)}
                """
                
                self._execute_batch_insert(insert_query, f"迁移 new_base_people 批次 {idx + 1}/{len(data_batches)}")
        
        logger.info(f"迁移完成 new_base_people: {len(df)} 条记录")
    
    def migrate_base_stock(self):
        """迁移证券基础信息表 - 创建Stock顶点"""
        query = """
        SELECT id, ticker, org_en, org, abbr, abbr_en, abbr_py, mkt_code, mkt_en, mkt, list_status, list_dt, list_edt
        FROM base_stock
        """
        
        df = pd.read_sql(query, self.mysql_conn)
        data_batches = [df[i:i + self.batch_size] for i in range(0, len(df), self.batch_size)]
        
        for idx, batch_df in enumerate(tqdm(data_batches, total=len(data_batches), desc="迁移 base_stock 数据")):
            values = []
            for _, row in batch_df.iterrows():
                vid = escape_string_for_nebula(row['ticker'])
                stock_code = escape_string_for_nebula(row['ticker'])
                org_en = escape_string_for_nebula(row['org_en'] if pd.notna(row['org_en']) else '')
                org = escape_string_for_nebula(row['org'] if pd.notna(row['org']) else '')
                abbr = escape_string_for_nebula(row['abbr'] if pd.notna(row['abbr']) else '')
                abbr_en = escape_string_for_nebula(row['abbr_en'] if pd.notna(row['abbr_en']) else '')
                abbr_py = escape_string_for_nebula(row['abbr_py'] if pd.notna(row['abbr_py']) else '')
                mkt_code = escape_string_for_nebula(row['mkt_code'] if pd.notna(row['mkt_code']) else '')
                mkt_en = escape_string_for_nebula(row['mkt_en'] if pd.notna(row['mkt_en']) else '')
                mkt = escape_string_for_nebula(row['mkt'] if pd.notna(row['mkt']) else '')
                list_status = escape_string_for_nebula(row['list_status'] if pd.notna(row['list_status']) else '')
                list_dt = escape_string_for_nebula(str(row['list_dt']) if pd.notna(row['list_dt']) else '')
                list_edt = escape_string_for_nebula(str(row['list_edt']) if pd.notna(row['list_edt']) else '')
                
                values.append(f"{vid}: ({stock_code}, {org_en}, {org}, {abbr}, {abbr_en}, {abbr_py}, {mkt_en}, {mkt}, {list_status}, {list_dt}, {list_edt})")
            
            if values:
                insert_query = f"""
                INSERT VERTEX Stock(stock_code, org_en, org, abbr, abbr_en, abbr_py, mkt_en, mkt, list_status, list_dt, list_edt) VALUES
                {', '.join(values)}
                """
                
                self._execute_batch_insert(insert_query, f"迁移 base_stock 批次 {idx + 1}/{len(data_batches)}")
        
        logger.info(f"迁移完成 base_stock: {len(df)} 条记录")
    
    def migrate_fin_report_date(self):
        """迁移财务定期报告公告日表 - 创建Report顶点"""
        query = """
        SELECT businessid, rpt, fp, q, fy, p, publish_date, report_type
        FROM fin_report_date
        """
        
        df = pd.read_sql(query, self.mysql_conn)
        data_batches = [df[i:i + self.batch_size] for i in range(0, len(df), self.batch_size)]
        
        for idx, batch_df in enumerate(tqdm(data_batches, total=len(data_batches), desc="迁移 fin_report_date 数据")):
            values = []
            for _, row in batch_df.iterrows():
                vid = escape_string_for_nebula(row['businessid'])
                businessid = escape_string_for_nebula(row['businessid'])
                rpt = escape_string_for_nebula(str(row['rpt']) if pd.notna(row['rpt']) else '')
                fp = escape_string_for_nebula(str(row['fp']) if pd.notna(row['fp']) else '')
                q = escape_string_for_nebula(str(row['q']) if pd.notna(row['q']) else '')
                fy = escape_string_for_nebula(str(row['fy']) if pd.notna(row['fy']) else '')
                p = escape_string_for_nebula(str(row['p']) if pd.notna(row['p']) else '')
                publish_date = escape_string_for_nebula(str(row['publish_date']) if pd.notna(row['publish_date']) else '')
                report_type = escape_string_for_nebula(row['report_type'] if pd.notna(row['report_type']) else '')
                
                values.append(f"{vid}: ({businessid}, {rpt}, {fp}, {q}, {fy}, {p}, {publish_date}, {report_type})")
            
            if values:
                insert_query = f"""
                INSERT VERTEX Report(businessid, rpt, fp, q, fy, p, publish_date, report_type) VALUES
                {', '.join(values)}
                """
                
                self._execute_batch_insert(insert_query, f"迁移 fin_report_date 批次 {idx + 1}/{len(data_batches)}")
        
        logger.info(f"迁移完成 fin_report_date: {len(df)} 条记录")
    
    def create_stock_company_relationships(self):
        """创建Stock和Company之间的关系"""
        query = """
        SELECT s.std_sch, c.ticker 
        FROM std_org s 
        JOIN base_stock c ON s.ref_company_id = c.csfid
        """
        df = pd.read_sql(query, self.mysql_conn)
        
        if df.empty:
            logger.info("没有找到Stock-Company关系数据")
            return
        
        data_batches = [df[i:i + self.batch_size] for i in range(0, len(df), self.batch_size)]
        
        for idx, batch_df in enumerate(tqdm(data_batches, total=len(data_batches), desc="创建 Stock-Company 关系")):
            values = []
            for _, row in batch_df.iterrows():
                company_vid = escape_string_for_nebula(row['std_sch'])
                stock_vid = escape_string_for_nebula(row['ticker'])
                values.append(f"{company_vid} -> {stock_vid}: ()")
            
            if values:
                insert_query = f"""
                INSERT EDGE ISSUES_STOCK() VALUES
                {', '.join(values)}
                """
                
                self._execute_batch_insert(insert_query, f"创建 Stock-Company 关系批次 {idx + 1}/{len(data_batches)}")
        
        logger.info(f"创建Stock-Company关系完成: {len(df)} 条记录")
    
    def create_report_relationships(self):
        """创建Report和Company/Stock之间的关系"""
        # Company-Report关系
        query1 = """
        SELECT r.businessid, c.std_sch,r.rpt
        FROM fin_report_date r
        JOIN std_org c ON LEFT(r.businessid,13) = c.ref_company_id
        """
        df1 = pd.read_sql(query1, self.mysql_conn)
        
        if not df1.empty:
            data_batches = [df1[i:i + self.batch_size] for i in range(0, len(df1), self.batch_size)]
            
            for idx, batch_df in enumerate(tqdm(data_batches, total=len(data_batches), desc="创建 Company-Report 关系")):
                values = []
                for _, row in batch_df.iterrows():
                    company_vid = escape_string_for_nebula(row['std_sch'])
                    report_vid = escape_string_for_nebula(row['businessid'])
                    rpt = escape_string_for_nebula(str(row['rpt']).strip() if pd.notna(row['rpt']) else '')
                    rank = self.calculate_rank_from_date(rpt)
                    values.append(f"{company_vid} -> {report_vid} @{rank}: ({rpt})")
                
                if values:
                    insert_query = f"""
                    INSERT EDGE PUBLISHES_REPORT() VALUES
                    {', '.join(values)}
                    """
                    
                    self._execute_batch_insert(insert_query, f"创建 Company-Report 关系批次 {idx + 1}/{len(data_batches)}")
        
        # Stock-Report关系
        query2 = """
        SELECT r.businessid, s.ticker,r.rpt
        FROM fin_report_date r
        JOIN base_stock s ON LEFT(r.businessid,13) = s.csfid
        """
        df2 = pd.read_sql(query2, self.mysql_conn)
        
        if not df2.empty:
            data_batches = [df2[i:i + self.batch_size] for i in range(0, len(df2), self.batch_size)]
            
            for idx, batch_df in enumerate(tqdm(data_batches, total=len(data_batches), desc="创建 Stock-Report 关系")):
                values = []
                for _, row in batch_df.iterrows():
                    stock_vid = escape_string_for_nebula(row['ticker'])
                    report_vid = escape_string_for_nebula(row['businessid'])
                    rpt = escape_string_for_nebula(str(row['rpt']).strip() if pd.notna(row['rpt']) else '')
                    rank = self.calculate_rank_from_date(rpt)
                    values.append(f"{stock_vid} -> {report_vid} @{rank}: ({rpt})")
                
                if values:
                    insert_query = f"""
                    INSERT EDGE HAS_REPORT() VALUES
                    {', '.join(values)}
                    """
                    
                    self._execute_batch_insert(insert_query, f"创建 Stock-Report 关系批次 {idx + 1}/{len(data_batches)}")
        
        logger.info("创建Report关系完成")
    
    def migrate_equity_parent_company(self):
        """迁移母公司情况表 - 创建PARENT_OF关系"""
        query = """
        WITH C AS (SELECT a.secu,a.ticker, a.rpt, a.parent_cat, a.parent_id, a.reg_std, a.unit, a.currency, a.capital, a.ratio, a.vote_ratio, s.std_sch as parent_orig
        FROM equity_parent_company a
        JOIN std_org s ON a.parent_id = s.ref_company_id and a.parent_orig != s.std_sch and a.parent_cat = 2)
        SELECT 
            b.org as tar_name,C.*
        FROM C
        JOIN base_stock b ON b.code = C.secu
        """
        df = pd.read_sql(query, self.mysql_conn)
        
        # 分别处理公司和个人数据
        company_data = []
        person_data = []
        
        for _, row in df.iterrows():
            if not is_valid_data(row['parent_orig'], row['tar_name']):
                continue
            if row['parent_cat'] == 2:  # 机构
                company_data.append(row)
           
        
        # 处理公司数据
        if company_data:
            # 首先插入可能不存在的公司顶点
            company_vertices = []
            for row in company_data:
                vid = escape_string_for_nebula(row['parent_orig'])
                company_name = escape_string_for_nebula(row['parent_orig'])
                company_vertices.append(f"{vid}: ({company_name}, \"\", \"\", \"\", \"\", \"\", \"\")")
            
            if company_vertices:
                # 分批插入顶点
                vertex_batches = [company_vertices[i:i + self.batch_size] for i in range(0, len(company_vertices), self.batch_size)]
                for idx, batch in enumerate(vertex_batches):
                    insert_vertex_query = f"""
                    INSERT VERTEX Company(company_name, std_en, cdtid, csfid, orgid, is_bond_issuer, is_listing) VALUES
                    {', '.join(batch)}
                    """
                    self._execute_batch_insert(insert_vertex_query, f"插入parent公司顶点批次 {idx + 1}")
            
            # 插入边
            edge_batches = [company_data[i:i + self.batch_size] for i in range(0, len(company_data), self.batch_size)]
            for idx, batch in enumerate(edge_batches):
                edge_values = []
                for row in batch:
                    parent_vid = escape_string_for_nebula(row['parent_orig'])
                    child_vid = escape_string_for_nebula(row['tar_name'])
                    ticker = escape_string_for_nebula(row['ticker'] if pd.notna(row['ticker']) else '')
                    rpt = escape_string_for_nebula(str(row['rpt']) if pd.notna(row['rpt']) else '')
                    rank = self.calculate_rank_from_date(rpt)

                    reg_std = escape_string_for_nebula(row['reg_std'] if pd.notna(row['reg_std']) else '')
                    unit = escape_string_for_nebula(row['unit'] if pd.notna(row['unit']) else '')
                    currency = escape_string_for_nebula(row['currency'] if pd.notna(row['currency']) else '')
                    capital = str(row['capital']) if pd.notna(row['capital']) else "0.0"
                    ratio = str(row['ratio']) if pd.notna(row['ratio']) else "0.0"
                    vote_ratio = str(row['vote_ratio']) if pd.notna(row['vote_ratio']) else "0.0"
                    
                    edge_values.append(f"{parent_vid} -> {child_vid} @{rank}: ({ticker}, {rpt}, {reg_std}, {unit}, {currency}, {capital}, {ratio}, {vote_ratio})")
                
                if edge_values:
                    insert_edge_query = f"""
                    INSERT EDGE PARENT_OF(ticker, rpt, reg_std, unit, currency, capital, ratio, vote_ratio) VALUES
                    {', '.join(edge_values)}
                    """
                    self._execute_batch_insert(insert_edge_query, f"插入PARENT_OF关系批次 {idx + 1}")
        
        logger.info(f"迁移完成 equity_parent_company: {len(df)} 条记录")
    
    def migrate_equity_subsidiary_base(self):
        """迁移子公司基本情况表 - 创建SUBSIDIARY_OF关系"""
        query = """
        WITH C AS (SELECT a.secu,a.ticker, a.rpt, s.std_sch as subs_orig, a.subs_cat,a.subs_id, a.reg_std, a.bizzplace_std,a.directrate ,a.indirectrate ,a.totalrate
        FROM equity_subsidiary_base a
        JOIN std_org s ON a.subs_id = s.ref_company_id and a.subs_orig != s.std_sch and a.subs_cat = 2)
        SELECT 
            b.org as tar_name,C.*
        FROM C
        JOIN base_stock b ON b.code = C.secu
        """
        
        df = pd.read_sql(query, self.mysql_conn)
        company_data = []
        for _, row in df.iterrows():
            if not is_valid_data(row['subs_orig'], row['tar_name']):
                continue
            if row['subs_cat'] == 2:  # 机构
                company_data.append(row)
        
        if company_data:
            # 插入可能不存在的公司顶点
            company_vertices = []
            for row in company_data:
                vid = escape_string_for_nebula(row['subs_orig'])
                company_name = escape_string_for_nebula(row['subs_orig'])
                company_vertices.append(f"{vid}: ({company_name}, \"\", \"\", \"\", \"\", \"\", \"\")")
            
            if company_vertices:
                vertex_batches = [company_vertices[i:i + self.batch_size] for i in range(0, len(company_vertices), self.batch_size)]
                for idx, batch in enumerate(vertex_batches):
                    insert_vertex_query = f"""
                    INSERT VERTEX Company(company_name, std_en, cdtid, csfid, orgid, is_bond_issuer, is_listing) VALUES
                    {', '.join(batch)}
                    """
                    self._execute_batch_insert(insert_vertex_query, f"插入subsidiary公司顶点批次 {idx + 1}")
            
            # 插入边
            edge_batches = [company_data[i:i + self.batch_size] for i in range(0, len(company_data), self.batch_size)]
            for idx, batch in enumerate(edge_batches):
                edge_values = []
                for row in batch:
                    sub_vid = escape_string_for_nebula(row['subs_orig'])
                    parent_vid = escape_string_for_nebula(row['tar_name'])
                    ticker = escape_string_for_nebula(row['ticker'] if pd.notna(row['ticker']) else '')
                    rpt = escape_string_for_nebula(str(row['rpt']) if pd.notna(row['rpt']) else '')
                    rank = self.calculate_rank_from_date(rpt)
                    reg_std = escape_string_for_nebula(row['reg_std'] if pd.notna(row['reg_std']) else '')
                    bizzplace_std = escape_string_for_nebula(row['bizzplace_std'] if pd.notna(row['bizzplace_std']) else '')
                    directrate = str(row['directrate']) if pd.notna(row['directrate']) else "0.0"
                    indirectrate = str(row['indirectrate']) if pd.notna(row['indirectrate']) else "0.0"
                    totalrate = str(row['totalrate']) if pd.notna(row['totalrate']) else "0.0"
                    
                    edge_values.append(f"{sub_vid} -> {parent_vid} @{rank}: ({ticker}, {rpt}, {reg_std}, {bizzplace_std}, {directrate}, {indirectrate}, {totalrate})")
                
                if edge_values:
                    insert_edge_query = f"""
                    INSERT EDGE SUBSIDIARY_OF(ticker, rpt, reg_std, bizzplace_std, directrate, indirectrate, totalrate) VALUES
                    {', '.join(edge_values)}
                    """
                    self._execute_batch_insert(insert_edge_query, f"插入SUBSIDIARY_OF关系批次 {idx + 1}")
        
        logger.info(f"迁移完成 equity_subsidiary_base: {len(df)} 条记录")
    
    def migrate_equity_customer(self):
        """迁移供应链重要客户表 - 创建CUSTOMER_OF关系"""
        query = """
        WITH C AS (SELECT a.secu,a.ticker, a.rpt, s.std_sch as customer_orig, a.customer_id, a.customer_cat, a.cy_sch, a.cy_en, a.unit_sch, a.unit_en, a.amount, a.rate, a.typ, a.age
        FROM equity_customer a
        JOIN std_org s ON a.customer_id = s.ref_company_id AND a.customer_orig != s.std_sch AND a.customer_cat = 2)
        SELECT 
            b.org as tar_name,C.*
        FROM C
        JOIN base_stock b ON b.code = C.secu
        """
        df = pd.read_sql(query, self.mysql_conn)
        
        person_data = []
        company_data = []
        for _, row in df.iterrows():
            if not is_valid_data(row['customer_orig'], row['tar_name']):
                continue
            if row['customer_cat'] == 1:  # 自然人
                person_data.append(row)
            elif row['customer_cat'] == 2:  # 机构
                company_data.append(row)
        
        # 处理公司数据
        if company_data:
            # 插入客户公司顶点
            company_vertices = []
            for row in company_data:
                vid = escape_string_for_nebula(row['customer_orig'])
                company_name = escape_string_for_nebula(row['customer_orig'])
                company_vertices.append(f"{vid}: ({company_name}, \"\", \"\", \"\", \"\", \"\", \"\")")
            
            if company_vertices:
                vertex_batches = [company_vertices[i:i + self.batch_size] for i in range(0, len(company_vertices), self.batch_size)]
                for idx, batch in enumerate(vertex_batches):
                    insert_vertex_query = f"""
                    INSERT VERTEX Company(company_name, std_en, cdtid, csfid, orgid, is_bond_issuer, is_listing) VALUES
                    {', '.join(batch)}
                    """
                    self._execute_batch_insert(insert_vertex_query, f"插入customer公司顶点批次 {idx + 1}")
            
            # 插入边
            edge_batches = [company_data[i:i + self.batch_size] for i in range(0, len(company_data), self.batch_size)]
            for idx, batch in enumerate(edge_batches):
                edge_values = []
                for row in batch:
                    customer_vid = escape_string_for_nebula(row['customer_orig'])
                    supplier_vid = escape_string_for_nebula(row['tar_name'])
                    ticker = escape_string_for_nebula(row['ticker'] if pd.notna(row['ticker']) else '')
                    rpt = escape_string_for_nebula(str(row['rpt']) if pd.notna(row['rpt']) else '')
                    rank = self.calculate_rank_from_date(rpt)
                    cy_sch = escape_string_for_nebula(row['cy_sch'] if pd.notna(row['cy_sch']) else '')
                    cy_en = escape_string_for_nebula(row['cy_en'] if pd.notna(row['cy_en']) else '')
                    unit_sch = escape_string_for_nebula(row['unit_sch'] if pd.notna(row['unit_sch']) else '')
                    unit_en = escape_string_for_nebula(row['unit_en'] if pd.notna(row['unit_en']) else '')
                    amount = str(row['amount']) if pd.notna(row['amount']) else "0.0"
                    rate = str(row['rate']) if pd.notna(row['rate']) else "0.0"
                    typ = escape_string_for_nebula(row['typ'] if pd.notna(row['typ']) else '')
                    age = escape_string_for_nebula(row['age'] if pd.notna(row['age']) else '')
                    
                    edge_values.append(f"{supplier_vid} -> {customer_vid} @{rank}: ({ticker}, {rpt}, {cy_sch}, {cy_en}, {unit_sch}, {unit_en}, {amount}, {rate}, {typ}, {age})")
                
                if edge_values:
                    insert_edge_query = f"""
                    INSERT EDGE CUSTOMER_OF(ticker, rpt, cy_sch, cy_en, unit_sch, unit_en, amount, rate, typ, age) VALUES
                    {', '.join(edge_values)}
                    """
                    self._execute_batch_insert(insert_edge_query, f"插入CUSTOMER_OF关系批次 {idx + 1}")
        
        logger.info(f"迁移完成 equity_customer: {len(df)} 条记录")
    
    def migrate_equity_supplier(self):
        """迁移供应链供应商表 - 创建SUPPLIES_TO关系"""
        query = """
        WITH C AS (SELECT a.secu,a.ticker, a.rpt,  s.std_sch as supplier_orig, a.supplier_id, a.supplier_cat,a.cy_sch, a.cy_en, a.unit_sch, a.unit_en, a.amount, a.rate, a.typ, a.age
        FROM equity_supplier a
        JOIN std_org s ON a.supplier_id = s.ref_company_id AND a.supplier_orig != s.std_sch AND a.supplier_cat = 2)
        SELECT 
            b.org as tar_name,C.*
        FROM C
        JOIN base_stock b ON b.code = C.secu
        """
        
        df = pd.read_sql(query, self.mysql_conn)
        
        person_data = []
        company_data = []
        for _, row in df.iterrows():
            if not is_valid_data(row['supplier_orig'], row['tar_name']):
                continue
            if row['supplier_cat'] == 1:  # 自然人
                person_data.append(row)
            elif row['supplier_cat'] == 2:  # 机构
                company_data.append(row)
        
        # 处理公司数据
        if company_data:
            # 插入供应商公司顶点
            company_vertices = []
            for row in company_data:
                vid = escape_string_for_nebula(row['supplier_orig'])
                company_name = escape_string_for_nebula(row['supplier_orig'])
                company_vertices.append(f"{vid}: ({company_name}, \"\", \"\", \"\", \"\", \"\", \"\")")
            
            if company_vertices:
                vertex_batches = [company_vertices[i:i + self.batch_size] for i in range(0, len(company_vertices), self.batch_size)]
                for idx, batch in enumerate(vertex_batches):
                    insert_vertex_query = f"""
                    INSERT VERTEX Company(company_name, std_en, cdtid, csfid, orgid, is_bond_issuer, is_listing) VALUES
                    {', '.join(batch)}
                    """
                    self._execute_batch_insert(insert_vertex_query, f"插入supplier公司顶点批次 {idx + 1}")
            
            # 插入边
            edge_batches = [company_data[i:i + self.batch_size] for i in range(0, len(company_data), self.batch_size)]
            for idx, batch in enumerate(edge_batches):
                edge_values = []
                for row in batch:
                    supplier_vid = escape_string_for_nebula(row['supplier_orig'])
                    object_vid = escape_string_for_nebula(row['tar_name'])
                    ticker = escape_string_for_nebula(row['ticker'] if pd.notna(row['ticker']) else '')
                    rpt = escape_string_for_nebula(str(row['rpt']) if pd.notna(row['rpt']) else '')
                    rank = self.calculate_rank_from_date(rpt)
                    cy_sch = escape_string_for_nebula(row['cy_sch'] if pd.notna(row['cy_sch']) else '')
                    cy_en = escape_string_for_nebula(row['cy_en'] if pd.notna(row['cy_en']) else '')
                    unit_sch = escape_string_for_nebula(row['unit_sch'] if pd.notna(row['unit_sch']) else '')
                    unit_en = escape_string_for_nebula(row['unit_en'] if pd.notna(row['unit_en']) else '')
                    amount = str(row['amount']) if pd.notna(row['amount']) else "0.0"
                    rate = str(row['rate']) if pd.notna(row['rate']) else "0.0"
                    typ = escape_string_for_nebula(row['typ'] if pd.notna(row['typ']) else '')
                    age = escape_string_for_nebula(row['age'] if pd.notna(row['age']) else '')
                    
                    edge_values.append(f"{supplier_vid} -> {object_vid} @{rank}: ({ticker}, {rpt}, {cy_sch}, {cy_en}, {unit_sch}, {unit_en}, {amount}, {rate}, {typ}, {age})")
                
                if edge_values:
                    insert_edge_query = f"""
                    INSERT EDGE SUPPLIES_TO(ticker, rpt, cy_sch, cy_en, unit_sch, unit_en, amount, rate, typ, age) VALUES
                    {', '.join(edge_values)}
                    """
                    self._execute_batch_insert(insert_edge_query, f"插入SUPPLIES_TO关系批次 {idx + 1}")
        
        logger.info(f"迁移完成 equity_supplier: {len(df)} 条记录")
    
    def migrate_related_transactions(self):
        """迁移关联交易表 - 创建RELATED_SALE和RELATED_PURCHASE关系"""
        # 关联销售
        sale_query = """
        WITH C AS (SELECT a.secu,a.ticker,a.rpt,s.std_sch as related_orig,a.related_cat,a.cy_orig,a.cy_code,a.cy_unit,a.content,a.amount_curr,a.rate_curr,a.amount_prev,a.rate_prev,a.related_id
        FROM related_sale a
        JOIN std_org s ON a.related_id = s.ref_company_id AND a.related_cat = 2)
        SELECT 
            b.org as tar_name,C.*
        FROM C
        JOIN base_stock b ON b.code = C.secu
        """
        
        df_sale = pd.read_sql(sale_query, self.mysql_conn)
        
        person_data = []
        company_data = []
        for _, row in df_sale.iterrows():
            if not is_valid_data(row['related_orig'], row['tar_name']):
                continue
            if row['related_cat'] == 1:  # 自然人   
                person_data.append(row)
            elif row['related_cat'] == 2:  # 机构
                company_data.append(row)

        # 处理公司销售数据
        if company_data:
            # 插入公司顶点
            company_vertices = []
            for row in company_data:
                vid = escape_string_for_nebula(row['related_orig'])
                company_name = escape_string_for_nebula(row['related_orig'])
                company_vertices.append(f"{vid}: ({company_name}, \"\", \"\", \"\", \"\", \"\", \"\")")
            
            if company_vertices:
                vertex_batches = [company_vertices[i:i + self.batch_size] for i in range(0, len(company_vertices), self.batch_size)]
                for idx, batch in enumerate(vertex_batches):
                    insert_vertex_query = f"""
                    INSERT VERTEX Company(company_name, std_en, cdtid, csfid, orgid, is_bond_issuer, is_listing) VALUES
                    {', '.join(batch)}
                    """
                    self._execute_batch_insert(insert_vertex_query, f"插入related_sale公司顶点批次 {idx + 1}")
            
            # 插入边
            edge_batches = [company_data[i:i + self.batch_size] for i in range(0, len(company_data), self.batch_size)]
            for idx, batch in enumerate(edge_batches):
                edge_values = []
                for row in batch:
                    buyer_vid = escape_string_for_nebula(row['related_orig'])
                    seller_vid = escape_string_for_nebula(row['tar_name'])
                    ticker = escape_string_for_nebula(row['ticker'] if pd.notna(row['ticker']) else '')
                    rpt = escape_string_for_nebula(str(row['rpt']) if pd.notna(row['rpt']) else '')
                    rank = self.calculate_rank_from_date(rpt)
                    cy_orig = escape_string_for_nebula(row['cy_orig'] if pd.notna(row['cy_orig']) else '')
                    cy_code = escape_string_for_nebula(row['cy_code'] if pd.notna(row['cy_code']) else '')
                    cy_unit = escape_string_for_nebula(row['cy_unit'] if pd.notna(row['cy_unit']) else '')
                    content = escape_string_for_nebula(row['content'] if pd.notna(row['content']) else '')
                    amount_curr = str(row['amount_curr']) if pd.notna(row['amount_curr']) else "0.0"
                    rate_curr = str(row['rate_curr']) if pd.notna(row['rate_curr']) else "0.0"
                    amount_prev = str(row['amount_prev']) if pd.notna(row['amount_prev']) else "0.0"
                    rate_prev = str(row['rate_prev']) if pd.notna(row['rate_prev']) else "0.0"
                    
                    edge_values.append(f"{seller_vid} -> {buyer_vid} @{rank}: ({ticker}, {rpt}, {cy_orig}, {cy_code}, {cy_unit}, {content}, {amount_curr}, {rate_curr}, {amount_prev}, {rate_prev})")
                
                if edge_values:
                    insert_edge_query = f"""
                    INSERT EDGE RELATED_SALE(ticker, rpt, cy_orig, cy_code, cy_unit, content, amount_curr, rate_curr, amount_prev, rate_prev) VALUES
                    {', '.join(edge_values)}
                    """
                    self._execute_batch_insert(insert_edge_query, f"插入RELATED_SALE关系批次 {idx + 1}")
        
        # 关联采购
        purchase_query = """
        WITH C AS (
        SELECT a.secu,a.ticker, a.rpt, s.std_sch AS related_orig,a.related_cat, a.cy_orig, a.cy_code, a.cy_unit,a.content, a.amount_curr, a.rate_curr, a.amount_prev, a.rate_prev,a.amount_limit, a.exceed,a.related_id
        FROM related_purchase a
        JOIN std_org s ON a.related_id = s.ref_company_id 
                    AND a.related_orig != s.std_sch 
                    AND a.related_cat != 0
        )
        SELECT 
            b.org as tar_name,C.*
        FROM C
        JOIN base_stock b ON b.code = C.secu
        """
        
        df_purchase = pd.read_sql(purchase_query, self.mysql_conn)
        
        person_data = []
        company_data = []
        for _, row in df_purchase.iterrows():
            if not is_valid_data(row['related_orig'], row['tar_name']):
                continue
            if row['related_cat'] == 1:  # 自然人
                person_data.append(row)
            elif row['related_cat'] == 2:  # 机构
                company_data.append(row)

        # 处理公司采购数据
        if company_data:
            # 插入公司顶点
            company_vertices = []
            for row in company_data:
                vid = escape_string_for_nebula(row['related_orig'])
                company_name = escape_string_for_nebula(row['related_orig'])
                company_vertices.append(f"{vid}: ({company_name}, \"\", \"\", \"\", \"\", \"\", \"\")")
            
            if company_vertices:
                vertex_batches = [company_vertices[i:i + self.batch_size] for i in range(0, len(company_vertices), self.batch_size)]
                for idx, batch in enumerate(vertex_batches):
                    insert_vertex_query = f"""
                    INSERT VERTEX Company(company_name, std_en, cdtid, csfid, orgid, is_bond_issuer, is_listing) VALUES
                    {', '.join(batch)}
                    """
                    self._execute_batch_insert(insert_vertex_query, f"插入related_purchase公司顶点批次 {idx + 1}")
            
            # 插入边
            edge_batches = [company_data[i:i + self.batch_size] for i in range(0, len(company_data), self.batch_size)]
            for idx, batch in enumerate(edge_batches):
                edge_values = []
                for row in batch:
                    buyer_vid = escape_string_for_nebula(row['related_orig'])
                    seller_vid = escape_string_for_nebula(row['tar_name'])
                    ticker = escape_string_for_nebula(row['ticker'] if pd.notna(row['ticker']) else '')
                    rpt = escape_string_for_nebula(str(row['rpt']) if pd.notna(row['rpt']) else '')
                    rank = self.calculate_rank_from_date(rpt)
                    cy_orig = escape_string_for_nebula(row['cy_orig'] if pd.notna(row['cy_orig']) else '')
                    cy_code = escape_string_for_nebula(row['cy_code'] if pd.notna(row['cy_code']) else '')
                    cy_unit = escape_string_for_nebula(row['cy_unit'] if pd.notna(row['cy_unit']) else '')
                    content = escape_string_for_nebula(row['content'] if pd.notna(row['content']) else '')
                    amount_curr = str(row['amount_curr']) if pd.notna(row['amount_curr']) else "0.0"
                    rate_curr = str(row['rate_curr']) if pd.notna(row['rate_curr']) else "0.0"
                    amount_prev = str(row['amount_prev']) if pd.notna(row['amount_prev']) else "0.0"
                    rate_prev = str(row['rate_prev']) if pd.notna(row['rate_prev']) else "0.0"
                    amount_limit = str(row['amount_limit']) if pd.notna(row['amount_limit']) else "0.0"
                    exceed = escape_string_for_nebula(row['exceed'] if pd.notna(row['exceed']) else '')
                    
                    edge_values.append(f"{seller_vid} -> {buyer_vid} @{rank}: ({ticker}, {rpt}, {cy_orig}, {cy_code}, {cy_unit}, {content}, {amount_curr}, {rate_curr}, {amount_prev}, {rate_prev}, {amount_limit}, {exceed})")
                
                if edge_values:
                    insert_edge_query = f"""
                    INSERT EDGE RELATED_PURCHASE(ticker, rpt, cy_orig, cy_code, cy_unit, content, amount_curr, rate_curr, amount_prev, rate_prev, amount_limit, exceed) VALUES
                    {', '.join(edge_values)}
                    """
                    self._execute_batch_insert(insert_edge_query, f"插入RELATED_PURCHASE关系批次 {idx + 1}")
        
        logger.info(f"迁移完成 related_sale: {len(df_sale)} 条记录")
        logger.info(f"迁移完成 related_purchase: {len(df_purchase)} 条记录")
    
    def migrate_fin_other_payment(self):
        """迁移重要的其他应付款表 - 创建OTHER_PAYMENT_TO关系"""
        query = """
        WITH C AS (SELECT a.secu,a.ticker, a.rpt, a.cy_orig, a.cy_code, a.cy_unit,s.std_sch as supplier_orig,a.amount, a.rate, a.supplier_id, a.supplier_cat
        FROM fin_other_payment a
        JOIN std_org s ON a.supplier_id = s.ref_company_id AND a.supplier_cat = 2)
        SELECT 
            b.org as tar_name,C.*
        FROM C
        JOIN base_stock b ON b.code = C.secu
        """
        df = pd.read_sql(query, self.mysql_conn)
        
        person_data = []
        company_data = []
        for _, row in df.iterrows():
            if not is_valid_data(row['supplier_orig'], row['tar_name']):
                continue
            if row['supplier_cat'] == 1:  # 自然人
                person_data.append(row)
            elif row['supplier_cat'] == 2:  # 机构
                company_data.append(row)
        
        # 处理公司数据
        if company_data:
            # 插入公司顶点
            company_vertices = []
            for row in company_data:
                vid = escape_string_for_nebula(row['supplier_orig'])
                company_name = escape_string_for_nebula(row['supplier_orig'])
                company_vertices.append(f"{vid}: ({company_name}, \"\", \"\", \"\", \"\", \"\", \"\")")
            
            if company_vertices:
                vertex_batches = [company_vertices[i:i + self.batch_size] for i in range(0, len(company_vertices), self.batch_size)]
                for idx, batch in enumerate(vertex_batches):
                    insert_vertex_query = f"""
                    INSERT VERTEX Company(company_name, std_en, cdtid, csfid, orgid, is_bond_issuer, is_listing) VALUES
                    {', '.join(batch)}
                    """
                    self._execute_batch_insert(insert_vertex_query, f"插入other_payment公司顶点批次 {idx + 1}")
            
            # 插入边
            edge_batches = [company_data[i:i + self.batch_size] for i in range(0, len(company_data), self.batch_size)]
            for idx, batch in enumerate(edge_batches):
                edge_values = []
                for row in batch:
                    payer_vid = escape_string_for_nebula(row['supplier_orig'])
                    payee_vid = escape_string_for_nebula(row['tar_name'])
                    ticker = escape_string_for_nebula(row['ticker'] if pd.notna(row['ticker']) else '')
                    rpt = escape_string_for_nebula(str(row['rpt']) if pd.notna(row['rpt']) else '')
                    rank = self.calculate_rank_from_date(rpt)
                    cy_orig = escape_string_for_nebula(row['cy_orig'] if pd.notna(row['cy_orig']) else '')
                    cy_code = escape_string_for_nebula(row['cy_code'] if pd.notna(row['cy_code']) else '')
                    cy_unit = escape_string_for_nebula(row['cy_unit'] if pd.notna(row['cy_unit']) else '')
                    amount = str(row['amount']) if pd.notna(row['amount']) else "0.0"
                    rate = str(row['rate']) if pd.notna(row['rate']) else "0.0"
                    
                    edge_values.append(f"{payer_vid} -> {payee_vid} @{rank}: ({ticker}, {rpt}, {cy_orig}, {cy_code}, {cy_unit}, {amount}, {rate})")
                
                if edge_values:
                    insert_edge_query = f"""
                    INSERT EDGE OTHER_PAYMENT_TO(ticker, rpt, cy_orig, cy_code, cy_unit, amount, rate) VALUES
                    {', '.join(edge_values)}
                    """
                    self._execute_batch_insert(insert_edge_query, f"插入OTHER_PAYMENT_TO关系批次 {idx + 1}")
        
        logger.info(f"迁移完成 fin_other_payment: {len(df)} 条记录")
    
    def migrate_related_ar(self):
        """迁移关联应收款项表 - 创建RELATED_AR_FROM关系"""
        query = """
        WITH C AS (SELECT a.secu,a.ticker, a.rpt, a.items_orig, a.items_sch, a.items_en, a.items_code,s.std_sch as related_orig, a.related_cat, a.cy_orig, a.cy_code, a.cy_unit,a.amount_curr, a.rate_curr, a.amount_prev, a.rate_prev,a.debt_curr,a.debt_prev, a.related_id
        FROM related_ar a
        JOIN std_org s ON a.related_id = s.ref_company_id AND a.related_cat = 2)
        SELECT 
            b.org as tar_name,C.*
        FROM C
        JOIN base_stock b ON b.code = C.secu
        """
        
        df = pd.read_sql(query, self.mysql_conn)

        person_data = []
        company_data = []
        for _, row in df.iterrows():
            if not is_valid_data(row['related_orig'], row['tar_name']):
                continue
            if row['related_cat'] == 1:  # 自然人
                person_data.append(row)
            elif row['related_cat'] == 2:  # 机构
                company_data.append(row)
        
        # 处理公司数据
        if company_data:
            # 插入公司顶点
            company_vertices = []
            for row in company_data:
                vid = escape_string_for_nebula(row['related_orig'])
                company_name = escape_string_for_nebula(row['related_orig'])
                company_vertices.append(f"{vid}: ({company_name}, \"\", \"\", \"\", \"\", \"\", \"\")")
            
            if company_vertices:
                vertex_batches = [company_vertices[i:i + self.batch_size] for i in range(0, len(company_vertices), self.batch_size)]
                for idx, batch in enumerate(vertex_batches):
                    insert_vertex_query = f"""
                    INSERT VERTEX Company(company_name, std_en, cdtid, csfid, orgid, is_bond_issuer, is_listing) VALUES
                    {', '.join(batch)}
                    """
                    self._execute_batch_insert(insert_vertex_query, f"插入related_ar公司顶点批次 {idx + 1}")
            
            # 插入边
            edge_batches = [company_data[i:i + self.batch_size] for i in range(0, len(company_data), self.batch_size)]
            for idx, batch in enumerate(edge_batches):
                edge_values = []
                for row in batch:
                    creditor_vid = escape_string_for_nebula(row['related_orig'])
                    debtor_vid = escape_string_for_nebula(row['tar_name'])
                    ticker = escape_string_for_nebula(row['ticker'] if pd.notna(row['ticker']) else '')
                    rpt = escape_string_for_nebula(str(row['rpt']) if pd.notna(row['rpt']) else '')
                    rank = self.calculate_rank_from_date(rpt)
                    items_orig = escape_string_for_nebula(row['items_orig'] if pd.notna(row['items_orig']) else '')
                    items_sch = escape_string_for_nebula(row['items_sch'] if pd.notna(row['items_sch']) else '')
                    items_en = escape_string_for_nebula(row['items_en'] if pd.notna(row['items_en']) else '')
                    items_code = escape_string_for_nebula(row['items_code'] if pd.notna(row['items_code']) else '')
                    cy_orig = escape_string_for_nebula(row['cy_orig'] if pd.notna(row['cy_orig']) else '')
                    cy_code = escape_string_for_nebula(row['cy_code'] if pd.notna(row['cy_code']) else '')
                    cy_unit = escape_string_for_nebula(row['cy_unit'] if pd.notna(row['cy_unit']) else '')
                    amount_curr = str(row['amount_curr']) if pd.notna(row['amount_curr']) else "0.0"
                    rate_curr = str(row['rate_curr']) if pd.notna(row['rate_curr']) else "0.0"
                    amount_prev = str(row['amount_prev']) if pd.notna(row['amount_prev']) else "0.0"
                    rate_prev = str(row['rate_prev']) if pd.notna(row['rate_prev']) else "0.0"
                    debt_curr = str(row['debt_curr']) if pd.notna(row['debt_curr']) else "0.0"
                    debt_prev = str(row['debt_prev']) if pd.notna(row['debt_prev']) else "0.0"
                    
                    edge_values.append(f"{debtor_vid} -> {creditor_vid} @{rank}: ({ticker}, {rpt}, {items_orig}, {items_sch}, {items_en}, {items_code}, {cy_orig}, {cy_code}, {cy_unit}, {amount_curr}, {rate_curr}, {amount_prev}, {rate_prev}, {debt_curr}, {debt_prev})")
                
                if edge_values:
                    insert_edge_query = f"""
                    INSERT EDGE RELATED_AR_FROM(ticker, rpt, items_orig, items_sch, items_en, items_code, cy_orig, cy_code, cy_unit, amount_curr, rate_curr, amount_prev, rate_prev, debt_curr, debt_prev) VALUES
                    {', '.join(edge_values)}
                    """
                    self._execute_batch_insert(insert_edge_query, f"插入RELATED_AR_FROM关系批次 {idx + 1}")
        
        logger.info(f"迁移完成 related_ar: {len(df)} 条记录")

    def run_full_migration(self):
        """执行完整的数据迁移"""
        try:
            logger.info("开始数据迁移...")
            
            # 连接数据库
            self.connect_databases()
            
            # 创建space和schema
            self.create_space_and_schema()
            
            # 清空数据
            # self.clear_space()
            
            # 迁移基础数据
            self.migrate_std_org()
            self.migrate_new_base_people()
            self.migrate_base_stock()
            self.migrate_fin_report_date()
            
            # 创建基础关系
            self.create_stock_company_relationships()
            # self.create_report_relationships()
            
            # 迁移关系数据
            self.migrate_equity_parent_company()
            self.migrate_equity_subsidiary_base()
            self.migrate_equity_customer()
            self.migrate_equity_supplier()
            self.migrate_related_transactions()

            self.migrate_fin_other_payment()
            self.migrate_related_ar()
            
            logger.info("数据迁移完成!")
            
        except Exception as e:
            logger.error(f"数据迁移失败: {e}")
            raise
        finally:
            self.close_connections()

# 使用示例
if __name__ == "__main__":
    # MySQL配置
    mysql_config = {
        'host': '10.100.0.28',
        'user': 'chinascope_admin',
        'password': 'PYB9pebc4qBdaZ',
        'database': 'chinascope',
        'charset': 'utf8mb4'
    }
    
    # NebulaGraph配置
    nebula_config = {
        'host': '10.100.0.205',
        'port': 9669,
        'user': 'root',
        'password': 'nebula'
    }
    
    # 创建转换器
    converter = MySQLToNebulaConverter(mysql_config, nebula_config)
    
    # 执行迁移
    converter.run_full_migration() 