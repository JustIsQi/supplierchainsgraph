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
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import hashlib,os
from pathlib import Path
import re

# 配置日志
def setup_logger():
    """设置日志记录"""
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    log_file_path =  '/data/true_nas/zfs_share1/yy/logs/JSONToNebula_batch.log'
    
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

def convert_currency_string_to_float(input_str: str) -> Tuple[float, str]:
    """
    将输入的字符型数值转换为float型数值和单位
    
    Args:
        input_str: 输入的字符串，如 "1000万元", "100,000.00元", "156,277,435股"
    
    Returns:
        Tuple[float, str]: (数值, 单位) 单位只能是"元"或"股"
    
    Examples:
        >>> convert_currency_string_to_float("1000万元")
        (10000000.0, "元")
        >>> convert_currency_string_to_float("100,000.00元")
        (100000.0, "元")
        >>> convert_currency_string_to_float("156,277,435股")
        (156277435.0, "股")
    """
    if not input_str or not isinstance(input_str, str):
        return 0,''
    
    # 去除前后空格
    input_str = input_str.replace(' ', '').strip()
    
    # 定义单位映射表，将各种货币单位统一转换为"元"
    currency_units = {
        '美金': '美元', '美元': '美元', '澳元': '澳元', '韩币': '韩元', '韩元': '韩元',
        '人民币': '元', 'RMB': '元', 'CNY': '元', 'USD': '元','比索':'比索',"马来西亚林吉特":"马来西亚林吉特",
        '日元': '日元', '欧元': '欧元', '英镑': '英镑', '港币': '港币','港元': '港元', '台币': '台币', '新币': '新币', '加元': '加元',"新加坡币":"新加坡币",'元': '元', '万元': '元', '亿元': '元', '千元': '元',
        '股': '股', '万股': '股', '亿股': '股', '千股': '股'
    }
    
    # 使用正则表达式匹配数字和单位
    # 匹配模式：数字部分（可能包含逗号分隔符） + 单位（非数字、非空格、非逗号、非小数点）
    pattern = r'([\d,]+\.?\d*)\s*([^\d\s,\.]+)'
    match = re.search(pattern, input_str)
    
    if not match:
        # 如果没有匹配到单位，尝试只匹配数字
        number_only_pattern = r'[\d,]+\.?\d*'
        number_match = re.search(number_only_pattern, input_str)
        if number_match:
            number_part = number_match.group().replace(',', '').replace(',','')
            try:
                numeric_value = float(number_part)
                return numeric_value, ""
            except ValueError:
                logger.error(f"无法将 '{number_part}' 转换为数字")
                return 0,''
        else:
            logger.error(f"无法解析输入字符串: {input_str}")
            return 0,''
    
    number_part = match.group(1)
    unit_part = match.group(2).strip()
    
    # 标准化单位
    normalized_unit = ""
    for key, value in currency_units.items():
        if key in unit_part:
            normalized_unit = value
            break
    
    if normalized_unit == "" and unit_part:
        # 如果检测到单位但不支持，返回空单位
        logger.warning(f"不支持的单位: {unit_part}，将返回空单位")
    
    # 处理数字部分，去除逗号
    number_part = number_part.replace(',', '')
    
    try:
        numeric_value = float(number_part)
    except ValueError:
        logger.error(f"无法将 '{number_part}' 转换为数字")
        return 0,''
    
    # 根据单位进行数值转换
    if '万' in unit_part:
        numeric_value *= 10000
    elif '亿' in unit_part:
        numeric_value *= 100000000
    elif '千' in unit_part:
        numeric_value *= 1000
    
    return numeric_value, normalized_unit

import re

def extract_number_with_commas(s):
    """
    从字符串中提取包含千位分隔符（英文或中文逗号）的数字，并解析为整数。
    如果输入为空字符串或无法提取有效数字，则返回 0。
    支持格式如：
        "499,200"     -> 499200
        "1,234,567"   -> 1234567
        "1，234，567"  -> 1234567（中文逗号）
        "价格：1,234,567元" -> 1234567
        "销量：499,200件" -> 499200
        "" -> 0
    参数:
        s (str): 输入字符串

    返回:
        int: 提取到的整数，若无效则返回 0
    """
    # 如果是空字符串，直接返回 0
    if not s:
        return 0

    # 移除所有非数字、非英文逗号、非中文逗号的字符
    cleaned = re.sub(r'[^\d,，]', '', s)
   
    if not cleaned:
        return 0
    cleaned = cleaned.replace('，', ',')
    # 移除所有逗号，得到纯数字字符串
    number_str = cleaned.replace(',', '')
    # 检查是否至少有一个数字
    if not number_str.isdigit():
        return 0
    # 转换为整数并返回
    try:
        return int(number_str)
    except ValueError:
        return 0  # 理论上不会触发，因为 isdigit() 已检查

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

def convert_string_to_cypher_datetime(date_str: str, include_time: bool = False) -> str:
    """
    将日期时间字符串转换为Cypher datetime格式
    
    Args:
        date_str: 日期时间字符串，支持多种格式：
                 - "2024-12-31"
                 - "2024-12-31 10:30:00"
                 - "2024/12/31"
                 - "2024/12/31 10:30:00"
                 - "20241231"
                 - "2024年12月31日"
                 - "2024-12-31T10:30:00"
                 - "2024-12-31T10:30:00Z"
        include_time: 是否包含时间部分，默认False（仅日期）
    
    Returns:
        str: Cypher datetime格式字符串
            - 日期格式: datetime("2024-12-31T00:00:00")
            - 日期时间格式: datetime("2024-12-31T10:30:00")
            如果解析失败，返回空字符串或None
    
    Examples:
        >>> convert_string_to_cypher_datetime("2024-12-31")
        'datetime("2024-12-31T00:00:00")'
        >>> convert_string_to_cypher_datetime("2024-12-31 10:30:00", include_time=True)
        'datetime("2024-12-31T10:30:00")'
    """
    if not date_str:
        return 'NULL'
    
    try:
        # 清理日期字符串
        date_clean = str(date_str).strip()
        
        # 移除引号
        date_clean = date_clean.replace('"', '').replace("'", '')
        
        # 移除中文字符
        import re
        date_clean = re.sub(r'[年月日]', '', date_clean)
        
        # 处理已包含 'T' 的 ISO 格式或包含时间的格式
        has_time_separator = 'T' in date_clean or (':' in date_clean and ' ' in date_clean)
        
        if has_time_separator:
            # 标准化为空格分隔的格式先处理
            if 'T' in date_clean:
                date_clean = date_clean.replace('T', ' ')
            # 移除时区信息（Z, +08:00等）
            date_clean = re.sub(r'[Z\+\-]\d{2}:?\d{2}.*$', '', date_clean).strip()
            # 现在按空格分隔处理
            parts = date_clean.split()
            if len(parts) >= 2:
                # 有日期和时间部分
                date_part = parts[0]
                time_part = parts[1]
                # 确保时间格式完整
                time_parts = time_part.split(':')
                if len(time_parts) < 3:
                    time_part = ':'.join(time_parts + ['00'] * (3 - len(time_parts)))
                date_clean = f"{date_part} {time_part}"
                try:
                    dt = datetime.strptime(date_clean, '%Y-%m-%d %H:%M:%S')
                except ValueError:
                    try:
                        dt = datetime.strptime(date_clean, '%Y-%m-%d %H:%M')
                    except ValueError:
                        # 降级为仅日期
                        dt = datetime.strptime(date_part, '%Y-%m-%d')
            else:
                # 只有日期部分，但包含T或特殊格式
                dt = datetime.strptime(parts[0], '%Y-%m-%d')
        else:
            # 尝试多种日期格式
            date_formats = [
                '%Y-%m-%d %H:%M:%S',      # 2024-12-31 10:30:00
                '%Y/%m/%d %H:%M:%S',      # 2024/12/31 10:30:00
                '%Y-%m-%d',               # 2024-12-31
                '%Y/%m/%d',               # 2024/12/31
                '%Y%m%d',                # 20241231
                '%Y-%m-%d %H:%M',        # 2024-12-31 10:30
                '%Y/%m/%d %H:%M',        # 2024/12/31 10:30
                '%Y-%m',                 # 2024-12
                '%Y/%m',                 # 2024/12
                '%Y'                     # 2024
            ]
            
            dt = None
            for fmt in date_formats:
                try:
                    if fmt == '%Y-%m' or fmt == '%Y/%m':
                        # 如果只有年月，补充日为01
                        dt = datetime.strptime(date_clean + '-01', '%Y-%m-%d')
                    elif fmt == '%Y':
                        # 如果只有年，补充为01-01
                        dt = datetime.strptime(date_clean + '-01-01', '%Y-%m-%d')
                    else:
                        dt = datetime.strptime(date_clean, fmt)
                    break
                except ValueError:
                    continue
            
            if dt is None:
                logger.warning(f"无法解析日期时间格式: {date_str}")
                return 'NULL'
        
        # 如果不包含时间，设置为00:00:00
        if not include_time and dt.hour == 0 and dt.minute == 0 and dt.second == 0:
            # 保持日期格式
            pass
        elif not include_time:
            # 如果原始字符串包含时间但include_time=False，则清零时间
            dt = dt.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # 格式化为ISO 8601格式
        iso_format = dt.strftime('%Y-%m-%dT%H:%M:%S')
        
        # 返回Cypher datetime格式
        return f'datetime("{iso_format}")'
        
    except Exception as e:
        logger.warning(f"转换日期时间格式时发生错误: {e}, 输入: {date_str}")
        return 'NULL'

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
    def __init__(self, nebula_config: Dict, space_name: str = ""):
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
        clean = {k: (v if v is not None and v != "null" and v != ":null" else '') for k, v in data.items()}
        return clean
    
    def convert_ratio_to_float(self,ratio:str):
        try:
            if not ratio:
                return 0.0
            if '/' in ratio:
                try:
                    clean = ratio.replace(',', '')
                    a, b = clean.split('/')
                    return float(a) / float(b)
                except:
                    return 0.0
            ratio = ratio.replace("%", "").replace(',','').replace('，','').replace(' ','')
            return float(ratio) / 100.0 if ratio != '' and float(ratio) <= 100.0 else 0.0
        except:
            logger.error(f'float解析异常：{ratio}')
            return 0.0
    
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

    def insert_shareholder_vertex(self, stock_data: Dict,company_data:Dict,report_date:str):
        """插入股权股本变更边，增发或回购等总股本会变化  Base_Stock_Info"""
        stock_data = self.transfer_data(stock_data)
        company_data = self.transfer_data(company_data)

        company_name = company_data.get("company_name",'')
        stock_code = stock_data.get('stock_code', '')
        stock_list_status = stock_data.get('stock_list_status', '')
        total_share_capital = stock_data.get('total_share_capital', '')
        # 元、股、万股
        circulating_share_capital = stock_data.get('circulating_share_capital', '')
        risk_warning_time = stock_data.get('risk_warning_time', '')
        cancel_risk_warning_time = stock_data.get('cancel_risk_warning_time', '')
        risk_warning_status = stock_data.get('risk_warning_status', '')

        total_share_capital, _ = convert_currency_string_to_float(total_share_capital)
        circulating_share_capital, _ = convert_currency_string_to_float(circulating_share_capital)
        currency = "股"

        if not stock_code:
            logger.warning("股票代码为空，跳过插入")
            return False

        rank = calculate_rank_from_date(report_date)

        report_date = convert_string_to_cypher_datetime(report_date)

        if len(stock_code.split(',')) == 1:
            # 单个股票代码处理
            stock_vid = self.genegerate_vid(stock_code)
            company_vid = self.genegerate_vid(company_name)
            
            if self.edge_exists("Stock",
                {"stock_code":stock_code},
                'Stock',
                {"stock_code":stock_code},
                "Base_Stock_Info",
                {"total_share_num":total_share_capital,"circulating_share_number":circulating_share_capital,
                    "stock_list_status":stock_list_status,"risk_warning_time":risk_warning_time,
                    "cancel_risk_warning_time":cancel_risk_warning_time,"risk_warning_status":risk_warning_status}
            ):
                return True
            
            # 插入股权股本信息边 - 使用VID
            query = f"""
            INSERT EDGE Base_Stock_Info(total_share_num, circulating_share_number, currency,risk_warning_time, cancel_risk_warning_time, risk_warning_status, stock_list_status,report_datetime) VALUES
            {escape_string_for_nebula(stock_vid)} -> {escape_string_for_nebula(company_vid)} @{rank}: ({total_share_capital}, {circulating_share_capital}, {escape_string_for_nebula(currency)}, 
            {escape_string_for_nebula(risk_warning_time)}, {escape_string_for_nebula(cancel_risk_warning_time)}, {escape_string_for_nebula(risk_warning_status)}, 
            {escape_string_for_nebula(stock_list_status)},{report_date})
            """
            
            success, _ = self.execute_query(query, {}, f"插入股权股本变更边: {stock_code}")
            if success:
                self.stats['edges_inserted'] += 1
        else:
            # 多个股票代码处理
            
            for idx, code in enumerate(stock_code.split(',')):
                code = code.strip()
                if not code:
                    continue
                    
                stock_vid = self.genegerate_vid(code)
                company_vid = self.genegerate_vid(company_name)
                
                if self.edge_exists("Stock",
                    {"stock_code":code},
                    'Stock',
                    {"stock_code":code},
                    "Base_Stock_Info",
                    {"total_share_num":total_share_capital,"circulating_share_number":circulating_share_capital,
                        "stock_list_status":stock_list_status,"risk_warning_time":risk_warning_time,
                        "cancel_risk_warning_time":cancel_risk_warning_time,"risk_warning_status":risk_warning_status}
                ):
                    logger.info(f"股权股本信息边已存在: {code}")
                    self.stats['edges_skipped'] += 1
                    continue
                
                query = f"""
                INSERT EDGE Base_Stock_Info(total_share_num, circulating_share_number, currency,risk_warning_time, cancel_risk_warning_time, risk_warning_status, stock_list_status,report_datetime) VALUES
                {escape_string_for_nebula(stock_vid)} -> {escape_string_for_nebula(company_vid)} @{rank}: ({total_share_capital}, {circulating_share_capital}, {escape_string_for_nebula(currency)}, 
                {escape_string_for_nebula(risk_warning_time)}, {escape_string_for_nebula(cancel_risk_warning_time)}, {escape_string_for_nebula(risk_warning_status)}, 
                {escape_string_for_nebula(stock_list_status)},{report_date})
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

        total_assets, total_assets_currency = convert_currency_string_to_float(total_assets)
        registered_capital, registered_capital_currency = convert_currency_string_to_float(registered_capital)
        
        
        if not company_name:
            logger.warning("公司名称为空，跳过插入关系边")
            return False
        
        # 计算rank
        rank = calculate_rank_from_date(report_date) if report_date else 0
        report_date = convert_string_to_cypher_datetime(report_date)
        company_vid = self.genegerate_vid(company_name)
        
        # 插入公司基础信息关系边 - 使用VID
        query = f"""
        INSERT EDGE Base_Company_Info(registration_place, business_place, industry, business_scope, company_qualification, is_bond_issuer, industry_level,total_assets,total_assets_currency,current_registered_capital_currency, current_registered_capital,report_datetime) VALUES
        {escape_string_for_nebula(company_vid)} -> {escape_string_for_nebula(company_vid)} @{rank}: ({escape_string_for_nebula(registration_place)},{escape_string_for_nebula(business_place)}, 
        {escape_string_for_nebula(industry)}, {escape_string_for_nebula(business_scope)}, {escape_string_for_nebula(company_qualification)}, {escape_string_for_nebula(is_bond_issuer)}, {escape_string_for_nebula(industry_level)},
         {int(total_assets)},{escape_string_for_nebula(total_assets_currency)}, {escape_string_for_nebula(registered_capital_currency)},{int(registered_capital)},{report_date})
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
        status_change_time = person_data.get("status_change_time", report_date)
        is_active = person_data.get('is_active') or True
        
        rank = calculate_rank_from_date(report_date)
        report_date = convert_string_to_cypher_datetime(report_date)
        compensation, currency = convert_currency_string_to_float(compensation)
        if not currency:
            currency = "元"
            if compensation < 1000:
                compensation = compensation * 10000

        if not person_name:
            logger.warning("人员姓名为空，跳过插入")
            return False

        if self.edge_exists("Person",
            {"person_name":person_name},
            'Company',
            {"company_name":company_name},
            "Position_Info",
            {"position":position,"is_active":is_active,"status_change_time":status_change_time,"personal_salary":compensation}
        ):
            return True

        person_vid = self.genegerate_vid(person_name)
        company_vid = self.genegerate_vid(company_name)

        # 插入人员职位状态边 - 使用VID
        query = f"""
        INSERT EDGE Position_Info(position, is_active, status_change_time, personal_salary,currency,report_datetime) VALUES
        {escape_string_for_nebula(person_vid)} -> {escape_string_for_nebula(company_vid)} @{rank}: ({escape_string_for_nebula(position)}, {is_active}, {escape_string_for_nebula(status_change_time)}, {compensation},{escape_string_for_nebula(currency)},{report_date})
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
        report_date = convert_string_to_cypher_datetime(report_date)
        shareholder_type = shareholder_data.get('shareholder_type', '')
        
        share_type = shareholder_data.get('share_type', '')
        
        currency = shareholder_data.get('currency', '')
        is_major_shareholder = shareholder_data.get('is_major_shareholder') or False

        report_period_change_amount = extract_number_with_commas(shareholder_data.get('report_period_change_amount', '0'))
        period_end_holdings = extract_number_with_commas(shareholder_data.get('period_end_holdings', '0'))
        
        share_percentage = self.convert_ratio_to_float(shareholder_data.get('share_percentage', ''))
        vote_percentage = self.convert_ratio_to_float(shareholder_data.get('vote_percentage', ''))
        shareholding_percentage = self.convert_ratio_to_float(shareholder_data.get('shareholding_percentage', ''))

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
                {"shareholder_type":shareholder_type,"shareholding_ratio":shareholding_percentage,"currency":currency,
                    "is_major_shareholder":is_major_shareholder,"report_period_exchange_amount":report_period_change_amount,
                    "holdings_of_period_end":period_end_holdings,"share_type":share_type,"share_ratio":share_percentage,
                    "vote_ratio":vote_percentage}
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
                {"shareholder_type":shareholder_type,"shareholding_ratio":shareholding_percentage,"currency":currency,
                    "is_major_shareholder":is_major_shareholder,"report_period_exchange_amount":report_period_change_amount,
                    "holdings_of_period_end":period_end_holdings,"share_type":share_type,"share_ratio":share_percentage,
                    "vote_ratio":vote_percentage}
            ):
                logger.info(f"控股关系边已存在: {shareholder_name} -> {company_name}")
                self.stats['edges_skipped'] += 1
                return True
            
        # 生成VID
        shareholder_vid = self.genegerate_vid(shareholder_name)
        company_vid = self.genegerate_vid(company_name)
        
        # 插入控股关系边 - 使用VID
        query = f"""
        INSERT EDGE Shareholder(shareholder_type, shareholding_ratio, currency, is_major_shareholder, report_period_exchange_amount, holdings_of_period_end, share_type, share_ratio, vote_ratio,report_datetime) VALUES
        {escape_string_for_nebula(shareholder_vid)} -> {escape_string_for_nebula(company_vid)} @{rank}: ({escape_string_for_nebula(shareholder_type)}, {shareholding_percentage}, 
        {escape_string_for_nebula(currency)}, {is_major_shareholder}, {report_period_change_amount}, {period_end_holdings}, {escape_string_for_nebula(share_type)}, 
        {share_percentage}, {vote_percentage},{report_date})
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
        
        is_consolidated = subsidiary_data.get('is_consolidated') or False
        
        investment_method = subsidiary_data.get('investment_method', '')
        registration_place = subsidiary_data.get('registration_place','')
        business_scope = subsidiary_data.get('business_scope', '')

        total_assets,total_assets_currency = convert_currency_string_to_float(subsidiary_data.get('total_assets', ''))
        registered_capital,registered_capital_currency = convert_currency_string_to_float(subsidiary_data.get('registered_capital', ''))
        investment_amount,investment_amount_currency = convert_currency_string_to_float(subsidiary_data.get('investment_amount', ''))

        vote_percentage = self.convert_ratio_to_float(subsidiary_data.get('vote_percentage', ''))
        ownership_percentage = self.convert_ratio_to_float(subsidiary_data.get('ownership_percentage', ''))
        
        self.insert_company_vertex({'company_name': subsidiary_name})
        self.insert_company_vertex({'company_name': parent_company})

        # 检查边是否已存在
        sub_edge_exists = self.edge_exists("Company",
            {"company_name":subsidiary_name},
            'Company',
            {"company_name":parent_company},
            "Subsidiary",
            {"is_wholly_owned":is_wholly_owned, "subsidiary_type":subsidiary_type, "vote_ratio":vote_percentage,
                "subsidiary_relationship":subsidiary_relationship, "shareholding_ratio":ownership_percentage,
                "is_consolidated":is_consolidated, "total_investment":investment_amount, "investment_method":investment_method}
        )

        parent_edge_exists = self.edge_exists("Company",
            {"company_name":parent_company},
            'Company',
            {"company_name":subsidiary_name},
            "Parent_Of",
            {"is_wholly_owned":is_wholly_owned, "subsidiary_type":subsidiary_type, "vote_ratio":vote_percentage,
                "subsidiary_relationship":subsidiary_relationship, "shareholding_ratio":ownership_percentage,
                "is_consolidated":is_consolidated, "total_investment":investment_amount, "investment_method":investment_method}
        )
        
        if sub_edge_exists and parent_edge_exists:
            logger.info(f"子母公司关系边已存在: {subsidiary_name} <-> {parent_company}")
            self.stats['edges_skipped'] += 2
            return True
        # 计算rank
        rank = calculate_rank_from_date(report_date or subsidiary_data.get('report_period', ''))
        report_date = convert_string_to_cypher_datetime(report_date)
        # 生成VID
        subsidiary_vid = self.genegerate_vid(subsidiary_name)
        parent_vid = self.genegerate_vid(parent_company)
        
        # 插入子公司关系边 - 使用VID
        subsidiary_query = f"""
        INSERT EDGE Subsidiary(is_wholly_owned, subsidiary_type, subsidiary_relationship, shareholding_ratio, is_consolidated, total_investment, investment_method, vote_ratio,currency,report_datetime) VALUES
        {escape_string_for_nebula(subsidiary_vid)} -> {escape_string_for_nebula(parent_vid)} @{rank}: ({is_wholly_owned}, {escape_string_for_nebula(subsidiary_type)}, {escape_string_for_nebula(subsidiary_relationship)}, 
        {ownership_percentage}, {is_consolidated}, {investment_amount}, {escape_string_for_nebula(investment_method)}, {vote_percentage},{escape_string_for_nebula(investment_amount_currency)},{report_date}) 
        """
        
        success, _ = self.execute_query(subsidiary_query, {}, f"插入子公司关系: {subsidiary_name} -> {parent_company}")
        if success:
            self.stats['edges_inserted'] += 1

        # 插入母公司关系边 - 使用VID
        parent_query = f"""
        INSERT EDGE Parent_Of(is_wholly_owned, subsidiary_type, subsidiary_relationship, shareholding_ratio, is_consolidated, total_investment, investment_method, vote_ratio,currency,report_datetime) VALUES
        {escape_string_for_nebula(parent_vid)} -> {escape_string_for_nebula(subsidiary_vid)} @{rank}: ({is_wholly_owned }, {escape_string_for_nebula(subsidiary_type)}, {escape_string_for_nebula(subsidiary_relationship)}, 
        {ownership_percentage}, {is_consolidated}, {investment_amount}, {escape_string_for_nebula(investment_method)}, {vote_percentage},{escape_string_for_nebula(investment_amount_currency)},{report_date})
        """
        
        success, _ = self.execute_query(parent_query, {}, f"插入母公司关系: {parent_company} -> {subsidiary_name}")
        if success:
            self.stats['edges_inserted'] += 1

        # 查询Base_Compang_Info最新边,upsert total_assets, registered_capital!!!!!!!
        query = f"""
        INSERT EDGE Base_Company_Info(registration_place, business_place, industry, business_scope, company_qualification, is_bond_issuer, industry_level, total_assets, current_registered_capital,current_registered_capital_currency,total_assets_currency,report_datetime) VALUES
        {escape_string_for_nebula(subsidiary_vid)} -> {escape_string_for_nebula(subsidiary_vid)} @{rank}: ({escape_string_for_nebula(registration_place)}, "", "", {escape_string_for_nebula(business_scope)}, "", "", "",
         {int(total_assets)}, {int(registered_capital)},{escape_string_for_nebula(registered_capital_currency)},{escape_string_for_nebula(total_assets_currency)},{report_date})
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
        relationship_percentage = self.convert_ratio_to_float(related_data.get('relationship_percentage', ''))
        business_scope = related_data.get('business_scope', '')

        if not related_company_name:
            return False
        
        rank = calculate_rank_from_date(report_date or related_data.get('report_period', ''))
        report_date = convert_string_to_cypher_datetime(report_date)
        if related_party_type == '自然人':
            # 关联方为自然人
            self.insert_person_vertex({'person_name': related_company_name})
            # 生成VID
            company_vid = self.genegerate_vid(company_name)
            related_person_vid = self.genegerate_vid(related_company_name)
            
            # 插入关联公司关系边 - 使用VID
            query = f"""
            INSERT EDGE Related_Company(relationship, related_ratio, business_scope,report_datetime) VALUES
            {escape_string_for_nebula(related_person_vid)} -> {escape_string_for_nebula(company_vid)} @{rank}: ({escape_string_for_nebula(relationship)}, {relationship_percentage}, 
            {escape_string_for_nebula(business_scope)},{report_date})
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
                {"relationship":relationship,"related_ratio":relationship_percentage,"business_scope":business_scope}
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
            INSERT EDGE Related_Company(relationship, related_ratio, business_scope,report_datetime) VALUES
            {escape_string_for_nebula(related_company_vid)} -> {escape_string_for_nebula(company_vid)} @{rank}: ({escape_string_for_nebula(relationship)}, {relationship_percentage}, 
            {escape_string_for_nebula(business_scope)},{report_date}) 
            """
            
            success, _ = self.execute_query(query, {}, f"插入关联公司关系: {company_name} -> {related_company_name}")
            if success:
                self.stats['edges_inserted'] += 1
        return success
    
    def insert_supplier_edge(self, supplier_data: Dict, company_name: str, report_date: str = None):
        """插入供应商关系边 (Suppiler)"""
        supplier_data = self.transfer_data(supplier_data)
        supplier_name = supplier_data.get('supplier_name', '')

        supply_percentage = self.convert_ratio_to_float(supplier_data.get('supply_percentage', ''))
        supply_amount,supply_amount_currency = convert_currency_string_to_float(supplier_data.get('supply_amount', ''))
        
        currency = supply_amount_currency if supply_amount_currency else supplier_data.get('currency', '') 
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
            {"supply_percentage":supply_percentage,"supplier_amount":supply_amount,"currency":currency,"supply_content":supply_content,"is_major_supplier":is_major_supplier}
        ):
            logger.info(f"供应商关系边已存在: {supplier_name} -> {company_name}")
            self.stats['edges_skipped'] += 1
            return True
            
        # 确保供应商顶点存在
        self.insert_company_vertex({'company_name': supplier_name})
        
        # 计算rank
        rank = calculate_rank_from_date(report_date or supplier_data.get('report_period', ''))
        report_date = convert_string_to_cypher_datetime(report_date)
        # 生成VID
        supplier_vid = self.genegerate_vid(supplier_name)
        company_vid = self.genegerate_vid(company_name)
        
        # 插入供应商关系边 - 使用VID
        query = f"""INSERT EDGE Suppiler(supply_ratio, supplier_amount, currency, supply_content, is_major_supplier,report_datetime) VALUES
        {escape_string_for_nebula(supplier_vid)} -> {escape_string_for_nebula(company_vid)} @{rank}: ({supply_percentage}, {supply_amount}, {escape_string_for_nebula(currency)},
         {escape_string_for_nebula(supply_content)}, {is_major_supplier},{report_date})
        """
        
        success, _ = self.execute_query(query, {}, f"插入供应关系: {supplier_name} -> {company_name}")
        if success:
            self.stats['edges_inserted'] += 1
        return success
    
    def insert_customer_edge(self, customer_data: Dict, company_name: str, report_date: str = None):
        """插入客户关系边 (Customer)"""
        customer_data = self.transfer_data(customer_data)
        customer_name = customer_data.get('customer_name', '')
        customer_percentage = self.convert_ratio_to_float(customer_data.get('customer_percentage', ''))
        if customer_percentage > 1:
            customer_percentage = 0.0
        customer_amount ,customer_amount_currency= convert_currency_string_to_float(customer_data.get('customer_amount', ''))
        currency = customer_amount_currency if customer_amount_currency else customer_data.get('currency', '')
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
        report_date = convert_string_to_cypher_datetime(report_date)
        # 生成VID
        customer_vid = self.genegerate_vid(customer_name)
        company_vid = self.genegerate_vid(company_name)
    
        # 插入客户关系边 - 使用VID
        query = f"""
        INSERT EDGE Customer(customer_proportion, sales_amount, currency, business_content, is_major_customer,report_datetime) VALUES
        {escape_string_for_nebula(customer_vid)} -> {escape_string_for_nebula(company_vid)} @{rank}: ({customer_percentage}, {customer_amount}, {escape_string_for_nebula(currency)}, 
        {escape_string_for_nebula(business_content)}, {is_major_customer},{report_date})
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

        revenue,revenue_currency = convert_currency_string_to_float(product_data.get('revenue', ''))
        revenue_currency = revenue_currency if revenue_currency else product_data.get('currency', '')

        cost,cost_currency = convert_currency_string_to_float(product_data.get('cost', ''))
        cost_currency = cost_currency if cost_currency else product_data.get('currency', '')

        gross_profit,gross_profit_currency = convert_currency_string_to_float(product_data.get('gross_profit', ''))
        gross_profit_currency = gross_profit_currency if gross_profit_currency else product_data.get('currency', '')

        revenue_percentage = self.convert_ratio_to_float(product_data.get('revenue_percentage', ''))
        gross_profit_margin = self.convert_ratio_to_float(product_data.get('gross_profit_margin', ''))

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
                "report_last_date":report_last_date,"business_description":business_description}
        ):
            logger.info(f"生产关系边已存在: {company_name} -> {product_name}")
            self.stats['edges_skipped'] += 1
            return True
        
        # 计算rank
        rank = calculate_rank_from_date(report_date or product_data.get('report_period', ''))
        report_date = convert_string_to_cypher_datetime(report_date)    
        # 生成VID
        company_vid = self.genegerate_vid(company_name)
        product_vid = self.genegerate_vid(product_name)

        # 插入主营业务构成边 - 使用VID
        query = f"""
        INSERT EDGE Main_Business_Composition(business_country,report_datetime,business_description, sale_revenue, revenue_share, gross_margin,gross_profits, business_cost, sale_revenue_currency,gross_profits_currency,business_cost_currency) VALUES
        {escape_string_for_nebula(company_vid)} -> {escape_string_for_nebula(product_vid)} @{rank}: ({escape_string_for_nebula(business_country)},{report_date},{escape_string_for_nebula(business_description)},
         {escape_string_for_nebula(revenue)}, {escape_string_for_nebula(revenue_percentage)}, {escape_string_for_nebula(gross_profit_margin)}, {escape_string_for_nebula(gross_profit)},
         {escape_string_for_nebula(cost)},{escape_string_for_nebula(revenue_currency)},{escape_string_for_nebula(gross_profit_currency)},{escape_string_for_nebula(cost_currency)})
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
            if company_name == "null":
                company_name = ""
            
            if company_name:
                self.insert_company_vertex(company_info)
                self.insert_base_company_edge(company_info, report_last_date)

                # 2. 插入股票信息及关系
                if 'stock_info' in data and data['stock_info']:
                    stock_info = data['stock_info']
                    company_info = data['company_info']
                    if self.insert_stock_vertex(stock_info):
                        stock_code = stock_info.get('stock_code', '')
                        if stock_code:
                            self.insert_shareholder_vertex(stock_info, company_info,report_last_date)
                
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
#     inserter = JSONToNebulaInserter(nebula_config,space_name="YXSupplyChains")

#     # with open("responses/半年报告_windanno_e1b7ee52-6fa0-5d33-bab5-799e9b671063原文_qwen_thinking.json", 'r', encoding='utf-8') as f:
#     #     json_data = json.load(f)
#     # inserter.run_insertion(json_data) 

#     root = Path('/data/true_nas/zfs_share1/yy/data/wind_anno_qwen_json')  # 把这里换成你要遍历的目录
#     all_files = [p for p in root.rglob('*') if p.is_file()]

#     # 已处理文件记录
#     processed_log_path = Path('/data/true_nas/zfs_share1/yy/logs/processed_filenames.txt')
#     os.makedirs(processed_log_path.parent, exist_ok=True)
#     processed_files = set()
#     if processed_log_path.exists():
#         try:
#             with open(processed_log_path, 'r', encoding='utf-8') as pf:
#                 for line in pf:
#                     line = line.strip()
#                     if line:
#                         processed_files.add(line)
#             logger.info(f"已加载已处理文件数量: {len(processed_files)}")
#         except Exception as e:
#             logger.warning(f"读取已处理文件列表失败: {e}")

#     # 批量写入控制：每处理100个刷新一次到文件
#     pending_to_flush = []

#     # 读取JSON文件
#     for file in all_files:
#         if not file.name.endswith('qwen_thinking.json'):
#             continue

#         file_abs = str(file)
#         if file_abs in processed_files:
#             continue

#         logger.info(f"开始插入文件: {file.name}")
#         with open(file, 'r', encoding='utf-8') as f:
#             json_data = json.load(f)
#             inserter.run_insertion(json_data)

#         # 记录为已处理
#         processed_files.add(file_abs)
#         pending_to_flush.append(file_abs)

#         if len(pending_to_flush) >= 100:
#             try:
#                 with open(processed_log_path, 'a', encoding='utf-8') as pf:
#                     pf.write("\n".join(pending_to_flush) + "\n")
#                 logger.info(f"已批量写入已处理文件数量: {len(pending_to_flush)}")
#             except Exception as e:
#                 logger.error(f"写入已处理文件列表失败: {e}")
#             finally:
#                 pending_to_flush = []

#     # 循环结束后写入剩余未刷新的记录
#     if pending_to_flush:
#         try:
#             with open(processed_log_path, 'a', encoding='utf-8') as pf:
#                 pf.write("\n".join(pending_to_flush) + "\n")
#             logger.info(f"已写入剩余已处理文件数量: {len(pending_to_flush)}")
#         except Exception as e:
#             logger.error(f"写入剩余已处理文件列表失败: {e}")