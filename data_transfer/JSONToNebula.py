#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
JSON Response to NebulaGraph Inserter
根据 response.json 和 schema 将提取的企业数据插入到 NebulaGraph 中
"""

import json
import logging
from nebula3.gclient.net import ConnectionPool
from nebula3.Config import Config
from typing import Dict, List, Any, Optional
from datetime import datetime
import hashlib,os
from pathlib import Path

# 配置日志
def setup_logger():
    """设置日志记录"""
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    log_file_path =  '/data/true_nas/zfs_share1/yy/logs/JSONToNebula.log'
    
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

def clean_percentage(percentage_str):
    """清理百分比字符串，返回数值"""
    if not percentage_str:
        return 0.0
    # 移除百分号和逗号
    cleaned = str(percentage_str).replace('%', '').replace(',', '')
    try:
        return float(cleaned)
    except:
        return 0.0

def clean_amount(amount_str):
    """清理金额字符串，提取数值"""
    if not amount_str:
        return 0.0
    # 移除逗号和其他非数字字符，保留小数点
    cleaned = str(amount_str).replace(',', '')
    # 提取数字部分
    import re
    numbers = re.findall(r'[\d.]+', cleaned)
    if numbers:
        try:
            return float(numbers[0])
        except:
            return 0.0
    return 0.0

def calculate_rank_from_date(date_str: str) -> int:
    """
    根据报告截止日期计算rank值
    日期越新，rank值越大（优先级越高）
    
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
        return days_diff
        
    except Exception as e:
        logger.warning(f"计算日期rank时发生错误: {e}, 日期: {date_str}")
        return 0

class JSONToNebulaInserter:
    def __init__(self, nebula_config: Dict, space_name: str = "RTSupplyChains"):
        """
        初始化插入器
        
        Args:
            nebula_config: NebulaGraph数据库配置
            space_name: NebulaGraph space名称
        """
        self.nebula_config = nebula_config
        self.space_name = space_name
        self.nebula_pool = None
        self.nebula_session = None
        
        # 统计计数器
        self.stats = {
            'vertices_inserted': 0,
            'vertices_skipped': 0,
            'edges_inserted': 0,
            'edges_skipped': 0
        }
    
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
    
    def print_stats(self):
        """打印插入统计信息"""
        logger.info("="*50)
        logger.info("数据插入统计:")
        logger.info(f"  顶点插入: {self.stats['vertices_inserted']}")
        logger.info(f"  顶点跳过(已存在): {self.stats['vertices_skipped']}")
        logger.info(f"  边插入: {self.stats['edges_inserted']}")
        logger.info(f"  边跳过(已存在): {self.stats['edges_skipped']}")
        logger.info(f"  总顶点处理: {self.stats['vertices_inserted'] + self.stats['vertices_skipped']}")
        logger.info(f"  总边处理: {self.stats['edges_inserted'] + self.stats['edges_skipped']}")
        logger.info("="*50)
    
    def execute_query(self, query: str, parameters: Dict = None, description: str = ""):
        """执行查询，支持参数化查询"""
        # try:
        # 记录原始查询（用于调试）
        
        # print("\n*********************************")
        # print(query)
        # print("*********************************\n")
        
        result = self.nebula_session.execute(query)
        if not result.is_succeeded():
            try:
                error_msg = result.error_msg()
            except UnicodeDecodeError:
                # 如果解码失败，尝试使用其他编码或忽略错误
                try:
                    error_msg = result._resp.error_msg.decode('utf-8', errors='ignore')
                except:
                    error_msg = "无法解码错误信息(编码问题)"
            
            logger.error(f"{description}失败")
            logger.error(f"错误信息: {error_msg}")
            logger.error(f"执行的查询: {query[:500]}{'...' if len(query) > 500 else ''}")
            
            # 根据错误类型给出建议
            if "SyntaxError" in error_msg:
                logger.error("语法错误建议: 检查查询语法和参数格式")
            elif "ExecutionError" in error_msg:
                logger.error("执行错误建议: 检查数据类型和约束条件")
            elif "Duplicate" in error_msg:
                logger.warning("数据重复: 可能是主键冲突")
            
            return False, error_msg
            
        # 记录成功信息
        if result.row_size() > 0:
            logger.info(f"{description}成功 - 影响行数: {result.row_size()}")
        else:
            logger.info(f"{description}成功")
            
        return True, result
            
        # except Exception as e:
        #     logger.error(f"{description}异常: {e}")
        #     logger.error(f"查询: {query[:500]}{'...' if len(query) > 500 else ''}")
        #     logger.exception("详细错误信息:")  # 这会打印完整的堆栈跟踪
        #     return False, str(e)
    
    def vertex_exists(self, vid: str) -> bool:
        """检查顶点是否存在"""
        try:
            query = f'MATCH (v) WHERE id(v) == {escape_string_for_nebula(vid)} RETURN v LIMIT 1'
            success, result = self.execute_query(query, {},f"检查顶点{vid}是否存在")
            if success and result.row_size() > 0:
                return True
            return False
        except Exception as e:
            logger.warning(f"检查顶点存在性时发生异常: {e}, 假设顶点不存在")
            return False
    
    def edge_exists(self, from_vid_tag: str, from_vid_properties: dict, to_vid_tag: str, to_vid_properties: dict, edge_type: str, edge_properties: dict) -> bool:
        """检查边是否存在"""
        try:
            # 构建属性字符串
            def build_properties_string(props):
                if not props:
                    return ""
                prop_parts = []
                for key, value in props.items():
                    prop_parts.append(f"{key}: {escape_string_for_nebula(value)}")
                return "{" + ", ".join(prop_parts) + "}"
            
            from_props_str = build_properties_string(from_vid_properties)
            to_props_str = build_properties_string(to_vid_properties)
            edge_props_str = build_properties_string(edge_properties)
            
            # MATCH (a:Company {company_name: '北大荒农垦集团有限公司'})-[e:PARENT_OF {ticker: "600598",rpt:"2016-06-30"}]->(b:Company {company_name: '黑龙江北大荒农业股份有限公司'}) RETURN e
            query = f'MATCH (a:{from_vid_tag} {from_props_str})-[e:{edge_type} {edge_props_str}]->(b:{to_vid_tag} {to_props_str}) RETURN e LIMIT 1'
            logger.info(f"Query: {query}")
            success, result = self.execute_query(query, {},f"检查边{from_vid_tag}-[{edge_type}]->{to_vid_tag}是否存在")
            if success and result.row_size() > 0:
                return True
            return False
        except Exception as e:
            logger.warning(f"检查边存在性时发生异常: {e}, 假设边不存在")
            return False

    def genegerate_vid(self,name:str):
        vid = hashlib.md5(name.encode("utf-8")).hexdigest()
        return vid

    def transfer_data(self,data:Dict):
        clean = {k: (v if v is not None else '') for k, v in data.items()}
        return clean
    
    def insert_company_vertex(self, company_data: Dict):
        """插入公司顶点"""
        company_data = self.transfer_data(company_data)
        company_name = company_data.get('company_name', '')
        if not company_name:
            logger.warning("公司名称为空，跳过插入")
            return False
            
        vid = self.genegerate_vid(company_name)
        
        # 检查顶点是否存在
        if self.vertex_exists(vid):
            logger.info(f"公司顶点已存在: {company_name}")
            self.stats['vertices_skipped'] += 1
            return True
            
        # 插入公司顶点 - 使用参数化查询
        query = f"""INSERT VERTEX Company(company_name, company_name_en, company_abbr) 
        VALUES {escape_string_for_nebula(vid)}: ({escape_string_for_nebula(company_name)}, {escape_string_for_nebula(company_data.get('company_name_en', ''))}, {escape_string_for_nebula(company_data.get('company_abbr', ''))})
        """
        
        # 参数字典
        parameters = {
            "vid": vid,
            "company_name": company_name,
            "company_name_en": company_data.get('company_name_en', ''),
            "company_abbr": company_data.get('company_abbr', '')
        }
        
        success, _ = self.execute_query(query, parameters, f"插入公司顶点: {company_name}")
        if success:
            self.stats['vertices_inserted'] += 1
        return success
    
    def insert_stock_vertex(self, stock_data: Dict):
        """插入股票顶点"""
        stock_data = self.transfer_data(stock_data)
        stock_code = stock_data.get('stock_code', '')
        stock_name = stock_data.get('stock_name', '')
        stock_type = stock_data.get('stock_type', '')
        exchange = stock_data.get('exchange', '')
        list_dt = stock_data.get('list_dt', '')
        list_edt = stock_data.get('list_edt', '')
          
        if not stock_code:
            logger.warning("股票代码为空，跳过插入")
            return False
        success = False
        if len(stock_code.split(',')) == 1:
            vid = self.genegerate_vid(stock_code)
            # 检查顶点是否存在
            if self.vertex_exists(vid):
                logger.info(f"股票顶点已存在: {stock_code}")
                self.stats['vertices_skipped'] += 1
                return True
                
            # 插入股票顶点 - 使用参数化查询
            query = f"""
            INSERT VERTEX Stock(stock_code, stock_name, stock_type, exchange, list_dt, list_edt) VALUES
            {escape_string_for_nebula(vid)}: ({escape_string_for_nebula(stock_code)}, {escape_string_for_nebula(stock_name)}, {escape_string_for_nebula(stock_type)}, {escape_string_for_nebula(exchange)}, {escape_string_for_nebula(list_dt)}, {escape_string_for_nebula(list_edt)})
            """
            
            success, _ = self.execute_query(query, {}, f"插入股票顶点: {stock_code}")
            if success:
                self.stats['vertices_inserted'] += 1
        else:
            for idx, code in enumerate(stock_code.split(',')):
                vid = self.genegerate_vid(code)
                stock_name_list = stock_name.split(',')
                list_dt_list = list_dt.split(',')

                if self.vertex_exists(vid):
                    logger.info(f"股票顶点已存在: {vid}")
                    self.stats['vertices_skipped'] += 1
                    continue

                # 准备参数
                current_stock_name = stock_name_list[idx] if idx < len(stock_name_list) else stock_name_list[0]
                current_list_dt = list_dt_list[idx] if idx < len(list_dt_list) else list_dt_list[0]
                
                query = f"""
                INSERT VERTEX Stock(stock_code, stock_name, stock_type, exchange, list_dt, list_edt) VALUES
                {escape_string_for_nebula(vid)}: ({escape_string_for_nebula(code)}, {escape_string_for_nebula(current_stock_name)}, {escape_string_for_nebula(stock_type)}, {escape_string_for_nebula(exchange)}, {escape_string_for_nebula(current_list_dt)}, {escape_string_for_nebula(list_edt)})
                """
                
                success, _ = self.execute_query(query, {}, f"插入股票顶点: {code}")
                if success:
                    self.stats['vertices_inserted'] += 1
                else:
                    logger.warning(f"插入股票顶点失败: {code}")
                    self.stats['vertices_skipped'] += 1
        return success

    def insert_shareholder_vertex(self, stock_data: Dict,report_date:str):
        """插入股权股本变更边，增发或回购等总股本会变化  Base_Stock_Info"""
        stock_data = self.transfer_data(stock_data)
        stock_code = stock_data.get('stock_code', '')
        stock_list_status = stock_data.get('stock_list_status', '')
        total_share_capital = stock_data.get('total_share_capital', '')
        circulating_share_capital = stock_data.get('circulating_share_capital', '')
        risk_warning_time = stock_data.get('risk_warning_time', '')
        cancel_risk_warning_time = stock_data.get('cancel_risk_warning_time', '')
        risk_warning_status = stock_data.get('risk_warning_status', '')

        if not stock_code:
            logger.warning("股票代码为空，跳过插入")
            return False

        rank = calculate_rank_from_date(report_date)

        if len(stock_code.split(',')) == 1:
            # 单个股票代码处理
            stock_vid = self.genegerate_vid(stock_code)
            
            if self.edge_exists("Stock",
                {"stock_code":stock_code},
                'Stock',
                {"stock_code":stock_code},
                "Base_Stock_Info",
                {"total_share_capital":total_share_capital,"circulating_share_capital":circulating_share_capital,
                    "stock_list_status":stock_list_status,"risk_warning_time":risk_warning_time,
                    "cancel_risk_warning_time":cancel_risk_warning_time,"risk_warning_status":risk_warning_status}
            ):
                return True
            
            # 插入股权股本信息边 - 使用VID
            query = f"""
            INSERT EDGE Base_Stock_Info(total_share_capital, circulating_share_capital, risk_warning_time, cancel_risk_warning_time, risk_warning_status, stock_list_status) VALUES
            {escape_string_for_nebula(stock_vid)} -> {escape_string_for_nebula(stock_vid)} @{rank}: ({escape_string_for_nebula(total_share_capital)}, {escape_string_for_nebula(circulating_share_capital)}, {escape_string_for_nebula(risk_warning_time)}, {escape_string_for_nebula(cancel_risk_warning_time)}, {escape_string_for_nebula(risk_warning_status)}, {escape_string_for_nebula(stock_list_status)})
            """
            
            success, _ = self.execute_query(query, {}, f"插入股权股本变更边: {stock_code}")
            if success:
                self.stats['edges_inserted'] += 1
        else:
            # 多个股票代码处理
            total_share_capital_list = total_share_capital.split(',') if total_share_capital else []
            circulating_share_capital_list = circulating_share_capital.split(',') if circulating_share_capital else []
            
            for idx, code in enumerate(stock_code.split(',')):
                code = code.strip()
                if not code:
                    continue
                    
                stock_vid = self.genegerate_vid(code)
                
                # 检查该股票代码的边是否存在
                current_total_capital = total_share_capital_list[idx] if idx < len(total_share_capital_list) else (total_share_capital_list[0] if total_share_capital_list else '')
                current_circulating_capital = circulating_share_capital_list[idx] if idx < len(circulating_share_capital_list) else (circulating_share_capital_list[0] if circulating_share_capital_list else '')
                
                if self.edge_exists("Stock",
                    {"stock_code":code},
                    'Stock',
                    {"stock_code":code},
                    "Base_Stock_Info",
                    {"total_share_capital":current_total_capital,"circulating_share_capital":current_circulating_capital,
                        "stock_list_status":stock_list_status,"risk_warning_time":risk_warning_time,
                        "cancel_risk_warning_time":cancel_risk_warning_time,"risk_warning_status":risk_warning_status}
                ):
                    logger.info(f"股权股本信息边已存在: {code}")
                    self.stats['edges_skipped'] += 1
                    continue
                
                query = f"""
                INSERT EDGE Base_Stock_Info(total_share_capital, circulating_share_capital, risk_warning_time, cancel_risk_warning_time, risk_warning_status, stock_list_status) VALUES
                {escape_string_for_nebula(stock_vid)} -> {escape_string_for_nebula(stock_vid)} @{rank}: ({escape_string_for_nebula(current_total_capital)}, {escape_string_for_nebula(current_circulating_capital)}, {escape_string_for_nebula(risk_warning_time)}, {escape_string_for_nebula(cancel_risk_warning_time)}, {escape_string_for_nebula(risk_warning_status)}, {escape_string_for_nebula(stock_list_status)})
                """
                
                success, _ = self.execute_query(query, {}, f"插入股权股本变更边: {code}")
                if success:
                    self.stats['edges_inserted'] += 1
                else:
                    logger.warning(f"插入股权股本变更边失败: {code}")
                    self.stats['edges_skipped'] += 1
        return True
    
    def insert_base_company_edge(self, company_info: str, report_date: str = None):
        """插入公司基础信息的关系边 (Base_Company_Info)"""
        company_info = self.transfer_data(company_info)

        company_name = company_info.get('company_name', '')

        registration_place = company_info.get('registration_place', '')
        business_place = company_info.get('business_place', '') 
        industry = company_info.get('industry', '')
        business_scope = company_info.get('business_scope', '')
        company_qualification = company_info.get('company_qualification', '')
        is_bond_issuer = "是" if company_info.get('is_bond_issuer', '') else "否"
        industry_level = company_info.get('industry', '')

        total_assets = company_info.get('total_assets', '')
        registered_capital = company_info.get('registered_capital', '')

        if not company_name:
            logger.warning("公司名称为空，跳过插入关系边")
            return False
        
        # 计算rank
        rank = calculate_rank_from_date(report_date) if report_date else 0
        company_vid = self.genegerate_vid(company_name)
        
        # 插入公司基础信息关系边 - 使用VID
        query = f"""
        INSERT EDGE Base_Company_Info(registration_place, business_place, industry, business_scope, company_qualification, is_bond_issuer, industry_level, current_total_assets, registered_capital) VALUES
        {escape_string_for_nebula(company_vid)} -> {escape_string_for_nebula(company_vid)} @{rank}: ({escape_string_for_nebula(registration_place)},{escape_string_for_nebula(business_place)}, {escape_string_for_nebula(industry)}, {escape_string_for_nebula(business_scope)}, {escape_string_for_nebula(company_qualification)}, {escape_string_for_nebula(is_bond_issuer)}, {escape_string_for_nebula(industry_level)}, {escape_string_for_nebula(total_assets)}, {escape_string_for_nebula(registered_capital)})
        """
        
        success, _ = self.execute_query(query, {}, f"插入公司基础信息关系: {company_name}")
        if success:
            self.stats['edges_inserted'] += 1
        return success
            
    def insert_person_vertex(self, person_data: Dict):
        """插入人员顶点"""
        person_data = self.transfer_data(person_data)
        person_name = person_data.get('person_name', '')
        if not person_name:
            logger.warning("人员姓名为空，跳过插入")
            return False
            
        vid = self.genegerate_vid(person_name)
        
        # 检查顶点是否存在
        if self.vertex_exists(vid):
            logger.info(f"人员顶点已存在: {person_name}")
            self.stats['vertices_skipped'] += 1
            return True
            
        # 插入人员顶点 - 使用参数化查询
        query = f"""
        INSERT VERTEX Person(person_name, person_name_en, birth, education_level, sex) VALUES
        {escape_string_for_nebula(vid)}: ({escape_string_for_nebula(person_name)}, {escape_string_for_nebula(person_data.get('person_name_en', ''))}, {escape_string_for_nebula(person_data.get('birth', ''))}, {escape_string_for_nebula(person_data.get('education_level', ''))}, {escape_string_for_nebula(person_data.get('sex', ''))})
        """   
        
        success, _ = self.execute_query(query, {}, f"插入人员顶点: {person_name}")
        if success:
            self.stats['vertices_inserted'] += 1
        return success
    
    def insert_position_status_edge(self, person_data: Dict, company_name: str, report_date: str):
        """插入人员职位状态边 Position_Info"""
        person_data = self.transfer_data(person_data)
        person_name = person_data.get('person_name', '')
        position = person_data.get('position', '')
        compensation = person_data.get("compensation", '')
        status_change_time = person_data.get("status_change_time", '')
        is_active = person_data.get('is_active') or True
        
        rank = calculate_rank_from_date(report_date)

        if not person_name:
            logger.warning("人员姓名为空，跳过插入")
            return False

        if self.edge_exists("Person",
            {"person_name":person_name},
            'Company',
            {"company_name":company_name},
            "Position_Info",
            {"position":position,"is_active":is_active,"status_change_time":report_date,"compensation":compensation}
        ):
            return True

        person_vid = self.genegerate_vid(person_name)
        company_vid = self.genegerate_vid(company_name)

        # 插入人员职位状态边 - 使用VID
        query = f"""
        INSERT EDGE Position_Info(position, is_active, status_change_time, compensation) VALUES
        {escape_string_for_nebula(person_vid)} -> {escape_string_for_nebula(company_vid)} @{rank}: ({escape_string_for_nebula(position)}, {is_active}, {escape_string_for_nebula(report_date)}, {escape_string_for_nebula(compensation)})
        """
        
        success, _ = self.execute_query(query, {}, f"插入人员职位状态边: {person_name} -> {company_name}")
        if success:
            self.stats['edges_inserted'] += 1
        return success
    
    def insert_product_vertex(self, product_data: Dict):
        """插入产品顶点"""
        product_data = self.transfer_data(product_data)
        product_name = product_data.get('product_name', '')
        business_type = product_data.get('business_type', '')
        if not product_name:
            logger.warning("产品名称为空，跳过插入")
            return False
            
        # 使用产品名称+公司名称作为唯一标识
        vid = self.genegerate_vid(product_name)
        # 检查顶点是否存在
        if self.vertex_exists(vid):
            logger.info(f"产品顶点已存在: {vid}")
            self.stats['vertices_skipped'] += 1
            return True
            
        # 插入产品顶点 - 使用参数化查询
        query = f"""
        INSERT VERTEX Product(product_name, business_type) VALUES
        {escape_string_for_nebula(vid)}: ({escape_string_for_nebula(product_name)}, {escape_string_for_nebula(business_type)})
        """
        
        success, _ = self.execute_query(query, {}, f"插入产品顶点: {vid}")
        if success:
            self.stats['vertices_inserted'] += 1
        return success
    
    def insert_control_stake_edge(self, shareholder_data: Dict, company_name: str, report_date: str = None):
        """插入控股关系边 (Shareholder)"""
        shareholder_data = self.transfer_data(shareholder_data)
        shareholder_name = shareholder_data.get('name', '')
         # 计算rank
        rank = calculate_rank_from_date(report_date or shareholder_data.get('report_period', ''))
        
        shareholder_type = shareholder_data.get('shareholder_type', '')
        shareholding_percentage = shareholder_data.get('shareholding_percentage', '')
        report_period_change_amount = shareholder_data.get('report_period_change_amount', '')
        period_end_holdings = shareholder_data.get('period_end_holdings', '')
        share_type = shareholder_data.get('share_type', '')
        share_percentage = shareholder_data.get('share_percentage', '')
        currency = shareholder_data.get('currency', '')
        is_major_shareholder = shareholder_data.get('is_major_shareholder') or False
        
        if not shareholder_name:
            return False
       
        if shareholder_type == '自然人':
            # 创建Person顶点
            self.insert_person_vertex({'person_name': shareholder_name})

            if self.edge_exists("Person",
                {"person_name":shareholder_name},
                'Company',
                {"company_name":company_name},
                'Shareholder',
                {"shareholder_type":shareholder_type,"shareholding_percentage":shareholding_percentage,"currency":currency,
                    "is_major_shareholder":is_major_shareholder,"report_period_change_amount":report_period_change_amount,
                    "period_end_holdings":period_end_holdings,"share_type":share_type,"share_percentage":share_percentage}
            ):
                logger.info(f"控股关系边已存在: {shareholder_name} -> {company_name}")
                self.stats['edges_skipped'] += 1
                return True
        else:
            # 创建Company顶点  
            self.insert_company_vertex({'company_name': shareholder_name})
            if self.edge_exists("Company",
                {"company_name":shareholder_name},
                'Company',
                {"company_name":company_name},
                'Shareholder',
                {"shareholder_type":shareholder_type,"shareholding_percentage":shareholding_percentage,"currency":currency,
                    "is_major_shareholder":is_major_shareholder,"report_period_change_amount":report_period_change_amount,
                    "period_end_holdings":period_end_holdings,"share_type":share_type,"share_percentage":share_percentage}
            ):
                logger.info(f"控股关系边已存在: {shareholder_name} -> {company_name}")
                self.stats['edges_skipped'] += 1
                return True
            
        # 生成VID
        shareholder_vid = self.genegerate_vid(shareholder_name)
        company_vid = self.genegerate_vid(company_name)
        
        # 插入控股关系边 - 使用VID
        query = f"""
        INSERT EDGE Shareholder(shareholder_type, shareholding_percentage, currency, is_major_shareholder, report_period_change_amount, period_end_holdings, share_type, share_percentage) VALUES
        {escape_string_for_nebula(shareholder_vid)} -> {escape_string_for_nebula(company_vid)} @{rank}: ({escape_string_for_nebula(shareholder_type)}, {escape_string_for_nebula(shareholding_percentage)}, {escape_string_for_nebula(currency)}, {is_major_shareholder}, {escape_string_for_nebula(report_period_change_amount)}, {escape_string_for_nebula(period_end_holdings)}, {escape_string_for_nebula(share_type)}, {escape_string_for_nebula(share_percentage)})
        """
        
        success, _ = self.execute_query(query, {}, f"插入控股关系: {shareholder_name} -> {company_name}")
        if success:
            self.stats['edges_inserted'] += 1
        return success
    
    def insert_subsidiary_edge(self, subsidiary_data: Dict, parent_company: str, report_date: str = None):
        """
        子母公司关系：Subsidiary、Parent_of，这里只包含这两种边，非全资不再单独用边展示，而用is_wholly_owned 这个属性来判断
        Subsidiary、Parent_of为双向边，属性值一致
        """
        subsidiary_data = self.transfer_data(subsidiary_data)
        subsidiary_name = subsidiary_data.get('subsidiary_name', '')
        if not subsidiary_name:
            return False
            
        # 根据持股比例判断关系类型
        is_wholly_owned = subsidiary_data.get('is_wholly_owned') or False
        subsidiary_type = subsidiary_data.get('subsidiary_type', '')
        subsidiary_relationship = subsidiary_data.get('subsidiary_relationship', '')
        ownership_percentage = subsidiary_data.get('ownership_percentage', '')
        is_consolidated = subsidiary_data.get('is_consolidated') or False
        investment_amount = subsidiary_data.get('investment_amount', '')
        investment_method = subsidiary_data.get('investment_method', '')

        registration_place = subsidiary_data.get('registration_place','')
        business_scope = subsidiary_data.get('business_scope', '')
        total_assets = subsidiary_data.get('total_assets', '')
        registered_capital = subsidiary_data.get('registered_capital', '')
        
        
        self.insert_company_vertex({'company_name': subsidiary_name})
        self.insert_company_vertex({'company_name': parent_company})

        # 检查边是否已存在
        sub_edge_exists = self.edge_exists("Company",
            {"company_name":subsidiary_name},
            'Company',
            {"company_name":parent_company},
            "Subsidiary",
            {"is_wholly_owned":is_wholly_owned, "subsidiary_type":subsidiary_type, 
                "subsidiary_relationship":subsidiary_relationship, "ownership_percentage":ownership_percentage,
                "is_consolidated":is_consolidated, "investment_amount":investment_amount, "investment_method":investment_method}
        )

        parent_edge_exists = self.edge_exists("Company",
            {"company_name":parent_company},
            'Company',
            {"company_name":subsidiary_name},
            "Parent_Of",
            {"is_wholly_owned":is_wholly_owned, "subsidiary_type":subsidiary_type, 
                "subsidiary_relationship":subsidiary_relationship, "ownership_percentage":ownership_percentage,
                "is_consolidated":is_consolidated, "investment_amount":investment_amount, "investment_method":investment_method}
        )
        
        if sub_edge_exists and parent_edge_exists:
            logger.info(f"子母公司关系边已存在: {subsidiary_name} <-> {parent_company}")
            self.stats['edges_skipped'] += 2
            return True
        # 计算rank
        rank = calculate_rank_from_date(report_date or subsidiary_data.get('report_period', ''))
        
        # 生成VID
        subsidiary_vid = self.genegerate_vid(subsidiary_name)
        parent_vid = self.genegerate_vid(parent_company)
        
        # 插入子公司关系边 - 使用VID
        subsidiary_query = f"""
        INSERT EDGE Subsidiary(is_wholly_owned, subsidiary_type, subsidiary_relationship, ownership_percentage, is_consolidated, investment_amount, investment_method) VALUES
        {escape_string_for_nebula(subsidiary_vid)} -> {escape_string_for_nebula(parent_vid)} @{rank}: ({is_wholly_owned}, {escape_string_for_nebula(subsidiary_type)}, {escape_string_for_nebula(subsidiary_relationship)}, {escape_string_for_nebula(ownership_percentage)}, {is_consolidated}, {escape_string_for_nebula(investment_amount)}, {escape_string_for_nebula(investment_method)})
        """
        
        success, _ = self.execute_query(subsidiary_query, {}, f"插入子公司关系: {subsidiary_name} -> {parent_company}")
        if success:
            self.stats['edges_inserted'] += 1

        # 插入母公司关系边 - 使用VID
        parent_query = f"""
        INSERT EDGE Parent_Of(is_wholly_owned, subsidiary_type, subsidiary_relationship, ownership_percentage, is_consolidated, investment_amount, investment_method) VALUES
        {escape_string_for_nebula(parent_vid)} -> {escape_string_for_nebula(subsidiary_vid)} @{rank}: ({is_wholly_owned }, {escape_string_for_nebula(subsidiary_type)}, {escape_string_for_nebula(subsidiary_relationship)}, {escape_string_for_nebula(ownership_percentage)}, {is_consolidated}, {escape_string_for_nebula(investment_amount)}, {escape_string_for_nebula(investment_method)})
        """
        
        success, _ = self.execute_query(parent_query, {}, f"插入母公司关系: {parent_company} -> {subsidiary_name}")
        if success:
            self.stats['edges_inserted'] += 1

        # 查询Base_Compang_Info最新边,upsert total_assets, registered_capital!!!!!!!
        query = f"""
        INSERT EDGE Base_Company_Info(registration_place, business_place, industry, business_scope, company_qualification, is_bond_issuer, industry_level, current_total_assets, registered_capital) VALUES
        {escape_string_for_nebula(subsidiary_vid)} -> {escape_string_for_nebula(subsidiary_vid)} @{rank}: ({escape_string_for_nebula(registration_place)}, "", "", {escape_string_for_nebula(business_scope)}, "", "", "", {escape_string_for_nebula(total_assets)}, {escape_string_for_nebula(registered_capital)})
        """
        
        success, _ = self.execute_query(query, {}, f"插入公司基础信息关系: {subsidiary_name}")
        if success:
            self.stats['edges_inserted'] += 1

        return success
    
    def insert_related_company_edge(self, related_data: Dict, company_name: str, report_date: str = None):
        """插入关联公司关系边  Related_Company"""
        related_data = self.transfer_data(related_data)
        related_company_name = related_data.get('related_party_name', '')
        related_party_type = related_data.get('related_party_type', '')
        relationship = related_data.get('relationship', '')
        relationship_percentage = related_data.get('relationship_percentage', '')
        business_scope = related_data.get('business_scope', '')

        if not related_company_name:
            return False
        
        rank = calculate_rank_from_date(report_date or related_data.get('report_period', ''))

        if related_party_type == '自然人':
            # 关联方为自然人
            self.insert_person_vertex({'person_name': related_company_name})
            # 生成VID
            company_vid = self.genegerate_vid(company_name)
            related_person_vid = self.genegerate_vid(related_company_name)
            
            # 插入关联公司关系边 - 使用VID
            query = f"""
            INSERT EDGE Related_Company(relationship, relationship_percentage, business_scope) VALUES
            {escape_string_for_nebula(related_person_vid)} -> {escape_string_for_nebula(company_vid)} @{rank}: ({escape_string_for_nebula(relationship)}, {escape_string_for_nebula(relationship_percentage)}, {escape_string_for_nebula(business_scope)})
            """
            success, _ = self.execute_query(query, {}, f"插入关联公司关系: {company_name} -> {related_company_name}")
            if success:
                self.stats['edges_inserted'] += 1
        else:
            #关联公司
            if self.edge_exists('Company',
                {"company_name":company_name},
                'Company',
                {"company_name":related_company_name},
                'Related_Company',
                {"relationship":relationship,"relationship_percentage":relationship_percentage,"business_scope":business_scope}
            ):
                logger.info(f"关联公司关系边已存在: {company_name} -> {related_company_name}")
                self.stats['edges_skipped'] += 1
                return True
                
            # 确保关联公司顶点存在
            self.insert_company_vertex({'company_name': related_company_name})
            # 生成VID
            company_vid = self.genegerate_vid(company_name)
            related_company_vid = self.genegerate_vid(related_company_name)
            
            # 插入关联公司关系边 - 使用VID
            query = f"""
            INSERT EDGE Related_Company(relationship, relationship_percentage, business_scope) VALUES
            {escape_string_for_nebula(related_company_vid)} -> {escape_string_for_nebula(company_vid)} @{rank}: ({escape_string_for_nebula(relationship)}, {escape_string_for_nebula(relationship_percentage)}, {escape_string_for_nebula(business_scope)})
            """
            
            success, _ = self.execute_query(query, {}, f"插入关联公司关系: {company_name} -> {related_company_name}")
            if success:
                self.stats['edges_inserted'] += 1
        return success
    
    def insert_supplier_edge(self, supplier_data: Dict, company_name: str, report_date: str = None):
        """插入供应商关系边 (Suppiler)"""
        supplier_data = self.transfer_data(supplier_data)
        supplier_name = supplier_data.get('supplier_name', '')

        supply_percentage = supplier_data.get('supply_percentage', '')
        supply_amount = supplier_data.get('supply_amount', '')
        currency = supplier_data.get('currency', '')
        supply_content = supplier_data.get('supply_content', '')
        is_major_supplier = True if supplier_data.get('is_major_supplier', False) else False    

        if not supplier_name:
            return False
            
        # 检查边是否已存在
        if self.edge_exists('Company',
            {"company_name":supplier_name},
            'Company',
            {"company_name":company_name},
            'Suppiler',
            {"supply_percentage":supply_percentage,"supply_amount":supply_amount,"currency":currency,"supply_content":supply_content,"is_major_supplier":is_major_supplier}
        ):
            logger.info(f"供应商关系边已存在: {supplier_name} -> {company_name}")
            self.stats['edges_skipped'] += 1
            return True
            
        # 确保供应商顶点存在
        self.insert_company_vertex({'company_name': supplier_name})
        
        # 计算rank
        rank = calculate_rank_from_date(report_date or supplier_data.get('report_period', ''))
        
        # 生成VID
        supplier_vid = self.genegerate_vid(supplier_name)
        company_vid = self.genegerate_vid(company_name)
        
        # 插入供应商关系边 - 使用VID
        query = f"""INSERT EDGE Suppiler(supply_percentage, supply_amount, currency, supply_content, is_major_supplier) VALUES
        {escape_string_for_nebula(supplier_vid)} -> {escape_string_for_nebula(company_vid)} @{rank}: ({escape_string_for_nebula(supply_percentage)}, {escape_string_for_nebula(supply_amount)}, {escape_string_for_nebula(currency)}, {escape_string_for_nebula(supply_content)}, {is_major_supplier})
        """
        
        success, _ = self.execute_query(query, {}, f"插入供应关系: {supplier_name} -> {company_name}")
        if success:
            self.stats['edges_inserted'] += 1
        return success
    
    def insert_customer_edge(self, customer_data: Dict, company_name: str, report_date: str = None):
        """插入客户关系边 (Customer)"""
        customer_data = self.transfer_data(customer_data)
        customer_name = customer_data.get('customer_name', '')
        customer_percentage = customer_data.get('customer_percentage', '')
        customer_amount = customer_data.get('customer_amount', '')
        currency = customer_data.get('currency', '')
        business_content = customer_data.get('business_content', '')
        is_major_customer = True if customer_data.get('is_major_customer', False) else False
        if not customer_name:
            return False
            
        # 检查边是否已存在
        if self.edge_exists('Company',
            {"company_name":customer_name},
            'Company',
            {"company_name":company_name},
            'Customer',
            {"customer_percentage":customer_percentage,"customer_amount":customer_amount,"currency":currency,
                "business_content":business_content,"is_major_customer":is_major_customer}
        ):
            logger.info(f"客户关系边已存在: {customer_name} -> {company_name}")
            self.stats['edges_skipped'] += 1
            return True
            
        # 确保客户顶点存在
        self.insert_company_vertex({'company_name': customer_name})
        # 计算rank
        rank = calculate_rank_from_date(report_date or customer_data.get('report_period', ''))
        
        # 生成VID
        customer_vid = self.genegerate_vid(customer_name)
        company_vid = self.genegerate_vid(company_name)
    
        # 插入客户关系边 - 使用VID
        query = f"""
        INSERT EDGE Customer(customer_percentage, customer_amount, currency, business_content, is_major_customer) VALUES
        {escape_string_for_nebula(customer_vid)} -> {escape_string_for_nebula(company_vid)} @{rank}: ({escape_string_for_nebula(customer_percentage)}, {escape_string_for_nebula(customer_amount)}, {escape_string_for_nebula(currency)}, {escape_string_for_nebula(business_content)}, {is_major_customer})
        """
        
        success, _ = self.execute_query(query, {}, f"插入客户关系: {customer_name} -> {company_name}")
        if success:
            self.stats['edges_inserted'] += 1
        return success
    
    
    def insert_produces_edge(self, product_data: Dict, company_name: str, report_date: str = None):
        """插入公司生产产品关系边 (Main_Business_Composition)"""
        product_data = self.transfer_data(product_data)
        product_name = product_data.get('product_name', '')

        business_country = product_data.get('business_country', '')
        revenue = product_data.get('revenue', '')
        revenue_percentage = product_data.get('revenue_percentage', '')
        gross_profit_margin = product_data.get('gross_profit_margin', '')
        cost = product_data.get('cost', '')
        gross_profit = product_data.get('gross_profit', '')
        currency = product_data.get('currency', '')
        report_last_date = product_data.get('report_last_date', '')
        business_description = product_data.get('business_description', '')

        if not product_name:
            return False
            
        # 检查边是否已存在
        if self.edge_exists('Company',
            {"company_name":company_name},
            'Company',
            {"company_name":product_name},
            'Main_Business_Composition',
            {"business_country":business_country,"revenue":revenue,"revenue_percentage":revenue_percentage,
                "gross_profit_margin":gross_profit_margin,"cost":cost,"gross_profit":gross_profit,
                "currency":currency,"report_last_date":report_last_date,"business_description":business_description}
        ):
            logger.info(f"生产关系边已存在: {company_name} -> {product_name}")
            self.stats['edges_skipped'] += 1
            return True
        
        # 计算rank
        rank = calculate_rank_from_date(report_date or product_data.get('report_period', ''))

        # 生成VID
        company_vid = self.genegerate_vid(company_name)
        product_vid = self.genegerate_vid(product_name)

        # 插入主营业务构成边 - 使用VID
        query = f"""
        INSERT EDGE Main_Business_Composition(business_country, revenue, revenue_percentage, gross_profit_margin, cost, gross_profit, currency, report_last_date, business_description) VALUES
        {escape_string_for_nebula(company_vid)} -> {escape_string_for_nebula(product_vid)} @{rank}: ({escape_string_for_nebula(business_country)}, {escape_string_for_nebula(revenue)}, {escape_string_for_nebula(revenue_percentage)}, {escape_string_for_nebula(gross_profit_margin)}, {escape_string_for_nebula(cost)}, {escape_string_for_nebula(gross_profit)}, {escape_string_for_nebula(currency)}, {escape_string_for_nebula(report_last_date)}, {escape_string_for_nebula(business_description)})
        """
        
        success, _ = self.execute_query(query, {}, f"插入主营业务构成关系: {company_name} -> {product_name}")
        if success:
            self.stats['edges_inserted'] += 1
        return success
    
    def insert_json_data(self, data: dict):
        """插入JSON中的数据"""
        # try:
            
        logger.info(f"开始插入JSON数据")
        
        # 获取报告截止日期
        report_last_date = data.get('report_last_date', '')
        
        # 1. 插入公司基本信息
        if 'company_info' in data and data['company_info']:
            company_info = data['company_info']
            company_name = company_info.get('company_name', '')
            
            if company_name:
                self.insert_company_vertex(company_info)
                self.insert_base_company_edge(company_info, report_last_date)

                # 2. 插入股票信息及关系
                if 'stock_info' in data and data['stock_info']:
                    stock_info = data['stock_info']
                    if self.insert_stock_vertex(stock_info):
                        stock_code = stock_info.get('stock_code', '')
                        if stock_code:
                            self.insert_shareholder_vertex(stock_info, report_last_date)
                
                # 3. 插入董监高信息
                if 'persons' in data and data['persons']:
                    for person in data['persons']:
                        self.insert_person_vertex(person)
                        self.insert_position_status_edge(person, company_name, report_last_date)
                
                # 4. 插入股东关系
                if 'shareholders' in data and data['shareholders']:
                    for shareholder in data['shareholders']:
                        self.insert_control_stake_edge(shareholder, company_name, report_last_date)
                
                # 5. 插入子公司/母公司关系
                if 'subsidiaries' in data and data['subsidiaries']:
                    for subsidiary in data['subsidiaries']:
                        self.insert_subsidiary_edge(subsidiary, company_name, report_last_date)
                
                # 6. 插入关联公司关系
                if 'related_companies' in data and data['related_companies']:
                    for related in data['related_companies']:
                        self.insert_related_company_edge(related, company_name, report_last_date)
                
                # 7. 插入供应商关系
                if 'major_suppliers' in data and data['major_suppliers']:
                    for supplier in data['major_suppliers']:
                        self.insert_supplier_edge(supplier, company_name, report_last_date)
                
                # 8. 插入客户关系
                if 'major_customers' in data and data['major_customers']:
                    for customer in data['major_customers']:
                        self.insert_customer_edge(customer, company_name, report_last_date)
                
                # 9. 插入主营业务构成
                if 'main_business_composition' in data and data['main_business_composition']:
                    for product in data['main_business_composition']:
                        if self.insert_product_vertex(product):
                            self.insert_produces_edge(product, company_name, report_last_date)
                
                logger.info(f"JSON数据插入完成: {company_name}")
            else:
                logger.error("公司名称为空，无法插入数据")
        else:
            logger.error("未找到公司基本信息")
                
        # except Exception as e:
        #     logger.error(f"插入JSON数据失败: {e}")
        #     raise
    
    def run_insertion(self, json_data: dict):
        """运行完整的数据插入流程"""
        # try:
        logger.info("开始数据插入...")
        
        # 连接数据库
        self.connect_database()
        
        # 插入数据
        self.insert_json_data(json_data)
        
        # 打印统计信息
        self.print_stats()
        
        logger.info("数据插入完成!")
            
        # except Exception as e:
        #     logger.error(f"数据插入失败: {e}")
        #     raise
        # finally:
        self.close_connection()

# 使用示例
# if __name__ == "__main__":
#     # NebulaGraph配置
#     nebula_config = {
#         'host': '10.100.0.205',
#         'port': 9669,
#         'user': 'root',
#         'password': 'nebula'
#     }
    
#     # 创建插入器
#     inserter = JSONToNebulaInserter(nebula_config)

#     # with open("responses/半年报告_windanno_e1b7ee52-6fa0-5d33-bab5-799e9b671063原文_qwen_thinking.json", 'r', encoding='utf-8') as f:
#     #     json_data = json.load(f)
#     # inserter.run_insertion(json_data) 

#     root = Path('/data/true_nas/zfs_share1/yy/code/supplierchainsgraph/responses')  # 把这里换成你要遍历的目录
#     all_files = [p for p in root.rglob('*') if p.is_file()]
#     # 读取JSON文件
#     for file in all_files:
#         if file.name.endswith('qwen_thinking.json'):
#             logger.info(f"开始插入文件: {file.name}")
#             with open(file, 'r', encoding='utf-8') as f:
#                 json_data = json.load(f)
#                 inserter.run_insertion(json_data) 