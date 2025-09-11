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

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
    def __init__(self, nebula_config: Dict, space_name: str = "supply_chain"):
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
    
    def execute_query(self, query: str, description: str = ""):
        """执行查询"""
        try:
            result = self.nebula_session.execute(query)
            if not result.is_succeeded():
                logger.error(f"{description}失败: {result.error_msg()}")
                logger.error(f"查询: {query}")
                return False, result.error_msg()
            logger.info(f"\n{description} \t\t成功，{result}\n")
            return True, result
        except Exception as e:
            logger.error(f"{description}异常: {e}")
            logger.error(f"查询: {query}")
            return False, str(e)
    
    def vertex_exists(self, vid: str) -> bool:
        """检查顶点是否存在"""
        try:
            query = f'MATCH (v) WHERE id(v) == {escape_string_for_nebula(vid)} RETURN v LIMIT 1'
            success, result = self.execute_query(query, f"检查顶点{vid}是否存在")
            if success and result.row_size() > 0:
                return True
            return False
        except Exception as e:
            logger.warning(f"检查顶点存在性时发生异常: {e}, 假设顶点不存在")
            return False
    
    def edge_exists(self, from_vid: str, to_vid: str, edge_type: str) -> bool:
        """检查边是否存在"""
        try:
            from_vid_escaped = escape_string_for_nebula(from_vid)
            to_vid_escaped = escape_string_for_nebula(to_vid)
            query = f'MATCH (a)-[e:{edge_type}]->(b) WHERE id(a) == {from_vid_escaped} AND id(b) == {to_vid_escaped} RETURN e LIMIT 1'
            logger.info(f"Query: {query}")
            success, result = self.execute_query(query, f"检查边{from_vid}-[{edge_type}]->{to_vid}是否存在")
            if success and result.row_size() > 0:
                return True
            return False
        except Exception as e:
            logger.warning(f"检查边存在性时发生异常: {e}, 假设边不存在")
            return False
    
    def insert_company_vertex(self, company_data: Dict):
        """插入公司顶点"""
        company_name = company_data.get('company_name', '')
        if not company_name:
            logger.warning("公司名称为空，跳过插入")
            return False
            
        vid = escape_string_for_nebula(company_name)
        
        # 检查顶点是否存在
        if self.vertex_exists(company_name):
            logger.info(f"公司顶点已存在: {company_name}")
            self.stats['vertices_skipped'] += 1
            return True
            
        # 插入公司顶点 - 根据nebula_schema.txt中的Company Tag字段
        company_name_escaped = escape_string_for_nebula(company_name)
        company_name_en = escape_string_for_nebula(company_data.get('company_name_en', ''))
        company_abbr = escape_string_for_nebula(company_data.get('company_abbr', ''))
        company_type = escape_string_for_nebula(company_data.get('company_type', ''))
        registration_place = escape_string_for_nebula(company_data.get('registration_place', ''))
        business_place = escape_string_for_nebula(company_data.get('business_place', ''))
        industry = escape_string_for_nebula(company_data.get('industry', ''))
        business_scope = escape_string_for_nebula(company_data.get('business_scope', ''))
        company_qualification = escape_string_for_nebula(company_data.get('company_qualification', ''))
        is_bond_issuer = escape_string_for_nebula("是" if company_data.get('is_bond_issuer', False) else "否")
        
        query = f"""
        INSERT VERTEX Company(company_name, std_en, cdtid, csfid, orgid, is_bond_issuer, is_listing, registration_place, business_place, company_qualification,company_abbr,company_type,business_scope,industry) VALUES
        {vid}: ({company_name_escaped}, {company_name_en}, "", "", "", {is_bond_issuer}, "", {registration_place}, {business_place}, {company_qualification}, {company_abbr}, {company_type}, {business_scope}, {industry})
        """
        success, _ = self.execute_query(query, f"插入公司顶点: {company_name}")
        if success:
            self.stats['vertices_inserted'] += 1
        return success
    
    def insert_stock_vertex(self, stock_data: Dict):
        """插入股票顶点"""
        stock_code = stock_data.get('stock_code', '')
        list_dt = stock_data.get('list_dt', '')
        stock_name = stock_data.get('stock_name', '')
        list_status = stock_data.get('list_status', '')
        list_edt = stock_data.get('list_edt', '')
        stock_type = stock_data.get('stock_type', '')
        exchange = stock_data.get('exchange', '')
            
        if not stock_code:
            logger.warning("股票代码为空，跳过插入")
            return False
        
        if len(stock_code.split(',')) == 1:
            vid = escape_string_for_nebula(stock_code)
            # 检查顶点是否存在
            if self.vertex_exists(stock_code):
                logger.info(f"股票顶点已存在: {stock_code}")
                self.stats['vertices_skipped'] += 1
                return True
                
            # 插入股票顶点
            
            query = f"""
            INSERT VERTEX Stock(stock_code, abbr, abbr_en, abbr_py, list_status, list_dt, list_edt,stock_type,exchange) VALUES
            {vid}: ({stock_code},  {stock_name}, "", "", {list_status}, {list_dt}, {list_edt},{stock_type},{exchange})
            """
            success, _ = self.execute_query(query, f"插入股票顶点: {stock_code}")
            if success:
                self.stats['vertices_inserted'] += 1
        else:
            for idx,code in enumerate(stock_code.split(',')):
                vid = code
                stock_name_list = stock_name.split(',')
                list_dt_list = list_dt.split(',')

                if self.vertex_exists(vid):
                    logger.info(f"股票顶点已存在: {vid}")
                    self.stats['vertices_skipped'] += 1
                    continue

                if (len(stock_code.split(',')) == len(stock_name_list)) and (len(stock_code.split(',')) == len(list_dt_list)):
                    query = f"""
                    INSERT VERTEX Stock(stock_code, abbr, abbr_en, abbr_py, list_status, list_dt, list_edt,stock_type,exchange) VALUES
                    {vid}: ({code},  {stock_name_list[idx]}, "", "", {list_status}, {list_dt_list[idx]}, {list_edt},{stock_type},{exchange})
                    """
                else:
                    query = f"""
                    INSERT VERTEX Stock(stock_code, abbr, abbr_en, abbr_py, list_status, list_dt, list_edt,stock_type,exchange) VALUES
                    {vid}: ({code},  {stock_name_list[0]}, "", "", {list_status}, {list_dt_list[0]}, {list_edt},{stock_type},{exchange})
                    """
                success, _ = self.execute_query(query, f"插入股票顶点: {code}")
                if success:
                    self.stats['vertices_inserted'] += 1
                else:
                    logger.warning(f"插入股票顶点失败: {code}")
                    self.stats['vertices_skipped'] += 1
        return success

    def insert_shareholder_vertex(self, stock_data: Dict):
        """插入股权股本变更边，增发或回购等总股本会变化"""
        stock_code = stock_data.get('stock_code', '')
        total_share_capital = stock_data.get('total_share_capital', '')
        circulating_share_capital = stock_data.get('circulating_share_capital', '')
        risk_warning_time = stock_data.get('risk_warning_time', '')
        cancel_risk_warning_time = stock_data.get('cancel_risk_warning_time', '')
        risk_warning_status = stock_data.get('risk_warning_status', '')

        if self.vertex_exists(stock_code):
            


    
    def insert_person_vertex(self, person_data: Dict):
        """插入人员顶点"""
        person_name = person_data.get('person_name', '')
        if not person_name:
            logger.warning("人员姓名为空，跳过插入")
            return False
            
        vid = escape_string_for_nebula(person_name)
        
        # 检查顶点是否存在
        if self.vertex_exists(person_name):
            logger.info(f"人员顶点已存在: {person_name}")
            self.stats['vertices_skipped'] += 1
            return True
            
        # 插入人员顶点
        person_name_escaped = escape_string_for_nebula(person_name)
        name_en = escape_string_for_nebula(person_data.get('proson_name_en', ''))
        birth = escape_string_for_nebula(person_data.get('birth', ''))
        education_level = escape_string_for_nebula(person_data.get('education_level', ''))
        sex = escape_string_for_nebula(person_data.get('sex', ''))
        
        query = f"""
        INSERT VERTEX Person(person_name, name_en, birth, ce_cd, ce_sch, ce_en, profq_code, profq_sch, profq_en, sex_sch, sex_en, til_sch, til_en, tilcd) VALUES
        {vid}: ({person_name_escaped}, {name_en}, {birth}, "", {education_level}, "", "", "", "", {sex}, "", "", "", "")
        """
        
        success, _ = self.execute_query(query, f"插入人员顶点: {person_name}")
        if success:
            self.stats['vertices_inserted'] += 1
        return success
    
    def insert_product_vertex(self, product_data: Dict, company_name: str):
        """插入产品顶点"""
        product_name = product_data.get('product_name', '')
        if not product_name:
            logger.warning("产品名称为空，跳过插入")
            return False
            
        # 使用产品名称+公司名称作为唯一标识
        vid = f"{company_name}_{product_name}"
        vid_escaped = escape_string_for_nebula(vid)
        
        # 检查顶点是否存在
        if self.vertex_exists(vid):
            logger.info(f"产品顶点已存在: {vid}")
            self.stats['vertices_skipped'] += 1
            return True
            
        # 插入产品顶点（映射到Product Tag）
        product_name_escaped = escape_string_for_nebula(product_name)
        business_type = escape_string_for_nebula(product_data.get('business_type', ''))
        revenue = escape_string_for_nebula(product_data.get('revenue', ''))
        revenue_percentage = escape_string_for_nebula(product_data.get('revenue_percentage', ''))
        gross_profit_margin = escape_string_for_nebula(product_data.get('gross_profit_margin', ''))
        cost = escape_string_for_nebula(product_data.get('cost', ''))
        gross_profit = escape_string_for_nebula(product_data.get('gross_profit', ''))
        currency = escape_string_for_nebula(product_data.get('currency', ''))
        business_description = escape_string_for_nebula(product_data.get('business_description', ''))
        report_period = escape_string_for_nebula(product_data.get('report_period', ''))
        
        query = f"""
        INSERT VERTEX Product(product_name, business_type, revenue, revenue_percentage, gross_profit_margin, cost, gross_profit, currency, business_description, report_period) VALUES
        {vid_escaped}: ({product_name_escaped}, {business_type}, {revenue}, {revenue_percentage}, {gross_profit_margin}, {cost}, {gross_profit}, {currency}, {business_description}, {report_period})
        """
        
        success, _ = self.execute_query(query, f"插入产品顶点: {vid}")
        if success:
            self.stats['vertices_inserted'] += 1
        return success
    
    def insert_control_stake_edge(self, shareholder_data: Dict, company_name: str, report_date: str = None):
        """插入控股关系边 (CONTROL_STAKE)"""
        shareholder_name = shareholder_data.get('name', '')
        if not shareholder_name:
            return False
            
        # 检查边是否已存在
        if self.edge_exists(shareholder_name, company_name, 'CONTROL_STAKE'):
            logger.info(f"控股关系边已存在: {shareholder_name} -> {company_name}")
            self.stats['edges_skipped'] += 1
            return True
            
        # 确保股东顶点存在
        shareholder_type = shareholder_data.get('shareholder_type', '')
        if shareholder_type == '自然人':
            # 创建Person顶点
            self.insert_person_vertex({'person_name': shareholder_name})
        else:
            # 创建Company顶点  
            self.insert_company_vertex({'company_name': shareholder_name})
            
        # 插入控股关系边
        from_vid = escape_string_for_nebula(shareholder_name)
        to_vid = escape_string_for_nebula(company_name)
        
        # 计算rank
        rank = calculate_rank_from_date(report_date or shareholder_data.get('report_period', ''))
        
        shareholder_type_escaped = escape_string_for_nebula(shareholder_type)
        shareholding_percentage = escape_string_for_nebula(shareholder_data.get('shareholding_percentage', ''))
        shareholding_amount = escape_string_for_nebula(shareholder_data.get('shareholding_amount', ''))
        shareholding_value = escape_string_for_nebula(shareholder_data.get('shareholding_value', ''))
        currency = escape_string_for_nebula(shareholder_data.get('currency', ''))
        is_major_shareholder = escape_string_for_nebula("实控人" if shareholder_data.get('is_major_shareholder', False) else "非实控人")
        
        query = f"""
        INSERT EDGE CONTROL_STAKE(shareholder_type, shareholder_percetage, shareholder_amount, shareholder_value, currency, is_major_shareholder) VALUES
        {from_vid} -> {to_vid} @{rank}: ({shareholder_type_escaped}, {shareholding_percentage}, {shareholding_amount}, {shareholding_value}, {currency}, {is_major_shareholder})
        """
        
        success, _ = self.execute_query(query, f"插入控股关系: {shareholder_name} -> {company_name}")
        if success:
            self.stats['edges_inserted'] += 1
        return success
    
    def insert_subsidiary_edge(self, subsidiary_data: Dict, parent_company: str, report_date: str = None):
        """插入子公司关系边"""
        subsidiary_name = subsidiary_data.get('subsidiary_name', '')
        if not subsidiary_name:
            return False
            
        # 根据持股比例判断关系类型
        ownership_percentage = subsidiary_data.get('ownership_percentage', '')
        ownership_float = clean_percentage(ownership_percentage)
        
        # 确定边类型
        edge_type = "SUBSIDIARY_OF" if ownership_float >= 100.0 or subsidiary_data.get('is_wholly_owned', True) else "NON_WHOLLY_SUBSIDIARY_OF"
        
        # 检查边是否已存在
        if self.edge_exists(parent_company, subsidiary_name, edge_type):
            logger.info(f"子公司关系边已存在: {parent_company} -[{edge_type}]-> {subsidiary_name}")
            self.stats['edges_skipped'] += 1
            return True
            
        # 确保子公司顶点存在
        subsidiary_info = {
            'company_name': subsidiary_name,
            'registration_place': subsidiary_data.get('registration_place', ''),
            'business_scope': subsidiary_data.get('business_scope', '')
        }
        self.insert_company_vertex(subsidiary_info)
        
        from_vid = escape_string_for_nebula(parent_company)
        to_vid = escape_string_for_nebula(subsidiary_name)
        
        # 计算rank
        rank = calculate_rank_from_date(report_date or subsidiary_data.get('report_period', ''))
        
        if ownership_float >= 100.0 or subsidiary_data.get('is_wholly_owned', True):
            # 全资子公司 - 使用 SUBSIDIARY_OF
            registration_place = escape_string_for_nebula(subsidiary_data.get('registration_place', ''))
            business_scope = escape_string_for_nebula(subsidiary_data.get('business_scope', ''))
            
            query = f"""
            INSERT EDGE SUBSIDIARY_OF(ticker, rpt, reg_std, bizzplace_std, directrate, indirectrate, totalrate) VALUES
            {to_vid} -> {from_vid} @{rank}: ("", "", {registration_place}, {business_scope}, {ownership_float}, 0.0, {ownership_float})
            """
        else:
            # 非全资子公司 - 使用 NON_WHOLLY_SUBSIDIARY_OF
            query = f"""
            INSERT EDGE NON_WHOLLY_SUBSIDIARY_OF(ticker, rpt, ratio, unit, gains, dividend, equity) VALUES
            {to_vid} -> {from_vid} @{rank}: ("", "", {ownership_float}, "", 0.0, 0.0, 0.0)
            """
        
        success, _ = self.execute_query(query, f"插入子公司关系: {parent_company} -> {subsidiary_name}")
        if success:
            self.stats['edges_inserted'] += 1
        return success
    
    def insert_related_company_edge(self, related_data: Dict, company_name: str, report_date: str = None):
        """插入关联公司关系边"""
        related_company_name = related_data.get('company_name', '')
        if not related_company_name:
            return False
            
        # 检查边是否已存在
        if self.edge_exists(company_name, related_company_name, 'JOINT_VENTURE_OF'):
            logger.info(f"关联公司关系边已存在: {company_name} -> {related_company_name}")
            self.stats['edges_skipped'] += 1
            return True
            
        # 确保关联公司顶点存在
        related_info = {
            'company_name': related_company_name,
            'business_scope': related_data.get('business_scope', '')
        }
        self.insert_company_vertex(related_info)
        
        # 插入关联关系边 - 使用 JOINT_VENTURE_OF
        from_vid = escape_string_for_nebula(company_name)
        to_vid = escape_string_for_nebula(related_company_name)
        
        # 计算rank
        rank = calculate_rank_from_date(report_date or related_data.get('report_period', ''))
        
        cooperation_type = escape_string_for_nebula(related_data.get('related_company_type', ''))
        relationship = escape_string_for_nebula(related_data.get('relationship', ''))
        relationship_percentage = escape_string_for_nebula(related_data.get('relationship_percentage', ''))
        business_scope = escape_string_for_nebula(related_data.get('business_scope', ''))
        
        query = f"""
        INSERT EDGE JOINT_VENTURE_OF(ticker, rpt, reg_sch, bizzplace_sch, directrable, indirectrable, totalrable) VALUES
        {to_vid} -> {from_vid} @{rank}: ("", "", "", "", {relationship_percentage}, "", {relationship_percentage})
        """
        
        success, _ = self.execute_query(query, f"插入合作关系: {company_name} -> {related_company_name}")
        if success:
            self.stats['edges_inserted'] += 1
        return success
    
    def insert_supplier_edge(self, supplier_data: Dict, company_name: str, report_date: str = None):
        """插入供应商关系边 (SUPPLIES_TO)"""
        supplier_name = supplier_data.get('supplier_name', '')
        if not supplier_name:
            return False
            
        # 检查边是否已存在
        if self.edge_exists(supplier_name, company_name, 'SUPPLIES_TO'):
            logger.info(f"供应商关系边已存在: {supplier_name} -> {company_name}")
            self.stats['edges_skipped'] += 1
            return True
            
        # 确保供应商顶点存在
        self.insert_company_vertex({'company_name': supplier_name})
        
        # 插入供应关系边
        from_vid = escape_string_for_nebula(supplier_name)
        to_vid = escape_string_for_nebula(company_name)
        
        # 计算rank
        rank = calculate_rank_from_date(report_date or supplier_data.get('report_period', ''))
        
        supply_percentage = clean_percentage(supplier_data.get('supply_percentage', ''))
        supply_amount = clean_amount(supplier_data.get('supply_amount', ''))
        currency = escape_string_for_nebula(supplier_data.get('currency', ''))
        supply_content = escape_string_for_nebula(supplier_data.get('supply_content', ''))
        is_major_supplier = True if supplier_data.get('is_major_supplier', False) else False    
        
        query = f"""
        INSERT EDGE SUPPLIES_TO(ticker, rpt, cy_sch, cy_en, unit_sch, unit_en, amount, rate, typ, age, supply_content, is_major_supplier) VALUES
        {from_vid} -> {to_vid} @{rank}: ("", "", {currency}, "", "", "", {supply_amount}, {supply_percentage}, "", "", {supply_content}, {is_major_supplier})
        """
        
        success, _ = self.execute_query(query, f"插入供应关系: {supplier_name} -> {company_name}")
        if success:
            self.stats['edges_inserted'] += 1
        return success
    
    def insert_customer_edge(self, customer_data: Dict, company_name: str, report_date: str = None):
        """插入客户关系边 (CUSTOMER_OF)"""
        customer_name = customer_data.get('customer_name', '')
        if not customer_name:
            return False
            
        # 检查边是否已存在
        if self.edge_exists(customer_name, company_name, 'CUSTOMER_OF'):
            logger.info(f"客户关系边已存在: {customer_name} -> {company_name}")
            self.stats['edges_skipped'] += 1
            return True
            
        # 确保客户顶点存在
        self.insert_company_vertex({'company_name': customer_name})
        
        # 插入客户关系边
        to_vid = escape_string_for_nebula(customer_name)
        from_vid = escape_string_for_nebula(company_name)
        
        # 计算rank
        rank = calculate_rank_from_date(report_date or customer_data.get('report_period', ''))
        
        customer_percentage = clean_percentage(customer_data.get('customer_percentage', ''))
        customer_amount = clean_amount(customer_data.get('customer_amount', ''))
        currency = escape_string_for_nebula(customer_data.get('currency', ''))
        business_content = escape_string_for_nebula(customer_data.get('business_content', ''))
        is_major_customer = True if customer_data.get('is_major_customer', False) else False
        
        query = f"""
        INSERT EDGE CUSTOMER_OF(ticker, rpt, cy_sch, cy_en, unit_sch, unit_en, amount, rate, typ, age, business_content, is_major_customer) VALUES
        {from_vid} -> {to_vid} @{rank}: ("", "", {currency}, "", "", "", {customer_amount}, {customer_percentage}, "", "", {business_content}, {is_major_customer})
        """
        
        success, _ = self.execute_query(query, f"插入客户关系: {customer_name} -> {company_name}")
        if success:
            self.stats['edges_inserted'] += 1
        return success
    
    def insert_stock_company_edge(self, stock_code: str, company_name: str, report_date: str = None):
        """插入股票-公司关系边 (ISSUES_STOCK)"""
        # 检查边是否已存在
        if self.edge_exists(company_name, stock_code, 'ISSUES_STOCK'):
            logger.info(f"股票发行关系边已存在: {company_name} -> {stock_code}")
            self.stats['edges_skipped'] += 1
            return True
            
        from_vid = escape_string_for_nebula(company_name)
        to_vid = escape_string_for_nebula(stock_code)
        
        # 计算rank
        rank = calculate_rank_from_date(report_date)
        
        query = f"""
        INSERT EDGE ISSUES_STOCK() VALUES
        {from_vid} -> {to_vid} @{rank}: ()
        """
        
        success, _ = self.execute_query(query, f"插入股票发行关系: {company_name} -> {stock_code}")
        if success:
            self.stats['edges_inserted'] += 1
        return success
    
    def insert_produces_edge(self, product_data: Dict, company_name: str, report_date: str = None):
        """插入公司生产产品关系边 (PRODUCES)"""
        product_name = product_data.get('product_name', '')
        if not product_name:
            return False
            
        # 产品顶点ID
        product_vid = f"{product_name}"
        
        # 检查边是否已存在
        if self.edge_exists(company_name, product_vid, 'PRODUCES'):
            logger.info(f"生产关系边已存在: {company_name} -> {product_name}")
            self.stats['edges_skipped'] += 1
            return True
        
        from_vid = escape_string_for_nebula(company_name)
        to_vid = escape_string_for_nebula(product_vid)
        
        # 计算rank
        rank = calculate_rank_from_date(report_date or product_data.get('report_period', ''))
        
        product_type = escape_string_for_nebula(product_name)
        business_type = escape_string_for_nebula(product_data.get('business_type', ''))
        revenue = escape_string_for_nebula(product_data.get('revenue', ''))
        revenue_percentage = escape_string_for_nebula(product_data.get('revenue_percentage', ''))
        gross_profit_margin = escape_string_for_nebula(product_data.get('gross_profit_margin', ''))
        cost = escape_string_for_nebula(product_data.get('cost', ''))
        gross_profit = escape_string_for_nebula(product_data.get('gross_profit', ''))
        currency = escape_string_for_nebula(product_data.get('currency', ''))
        report_period = escape_string_for_nebula(product_data.get('report_period', ''))
        business_description = escape_string_for_nebula(product_data.get('business_description', ''))
        
        query = f"""
        INSERT EDGE PRODUCES(product_type, business_type, revenue, revenue_percentage, gross_profit_margin, cost, gross_profit, currency, report_period, business_description) VALUES
        {from_vid} -> {to_vid} @{rank}: ({product_type}, {business_type}, {revenue}, {revenue_percentage}, {gross_profit_margin}, {cost}, {gross_profit}, {currency}, {report_period}, {business_description})
        """
        
        success, _ = self.execute_query(query, f"插入生产关系: {company_name} -> {product_name}")
        if success:
            self.stats['edges_inserted'] += 1
        return success
    
    def insert_json_data(self, data: dict):
        """插入JSON中的数据"""
        try:
            
            logger.info(f"开始插入JSON数据")
            
            # 获取报告截止日期
            report_last_date = data.get('report_last_date', '')
            
            # 1. 插入公司基本信息
            if 'company_info' in data and data['company_info']:
                company_info = data['company_info']
                company_name = company_info.get('company_name', '')
                
                if company_name:
                    self.insert_company_vertex(company_info)
                    
                    # 2. 插入股票信息及关系
                    if 'stock_info' in data and data['stock_info']:
                        stock_info = data['stock_info']
                        if self.insert_stock_vertex(stock_info):
                            stock_code = stock_info.get('stock_code', '')
                            if stock_code:
                                self.insert_stock_company_edge(stock_code, company_name, report_last_date)
                    
                    # 3. 插入人员信息
                    if 'persons' in data and data['persons']:
                        for person in data['persons']:
                            self.insert_person_vertex(person)
                    
                    # 4. 插入股东关系
                    if 'major_shareholders' in data and data['major_shareholders']:
                        for shareholder in data['major_shareholders']:
                            self.insert_control_stake_edge(shareholder, company_name, report_last_date)
                    
                    # 5. 插入子公司关系
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
                            if self.insert_product_vertex(product, company_name):
                                self.insert_produces_edge(product, company_name, report_last_date)
                    
                    logger.info(f"JSON数据插入完成: {company_name}")
                else:
                    logger.error("公司名称为空，无法插入数据")
            else:
                logger.error("未找到公司基本信息")
                
        except Exception as e:
            logger.error(f"插入JSON数据失败: {e}")
            raise
    
    def run_insertion(self, json_data: dict):
        """运行完整的数据插入流程"""
        try:
            logger.info("开始数据插入...")
            
            # 连接数据库
            self.connect_database()
            
            # 插入数据
            self.insert_json_data(json_data)
            
            # 打印统计信息
            self.print_stats()
            
            logger.info("数据插入完成!")
            
        except Exception as e:
            logger.error(f"数据插入失败: {e}")
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
    
    # 创建插入器
    inserter = JSONToNebulaInserter(nebula_config)
    
    # 读取JSON文件
    with open('response.json', 'r', encoding='utf-8') as f:
        json_data = json.load(f)
    
    # 执行插入
    inserter.run_insertion(json_data) 