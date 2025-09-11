import os
import sys
from pathlib import Path
import logging
from collections import defaultdict, namedtuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from functools import lru_cache

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('company_norm_processing_optimized.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

add_path = str(Path(__file__).parent.parent)
sys.path.append(add_path)
os.chdir(add_path)

from utils.es import search_documents
from utils.mysql_util import chinascope_search, wind_search
from utils.mongo import search_company_data, insert_data, upsert_data
from tqdm import tqdm

# 数据结构优化：使用namedtuple提高性能
CompanyInfo = namedtuple('CompanyInfo', ['tar_name', 'abbr', 'subs_orig'])
BaseInfo = namedtuple('BaseInfo', ['stock_code', 'zh_company_name', 'exchange_name', 'search_company_name'])

class CompanyProcessor:
    def __init__(self):
        # 缓存机制
        self.norm_company_cache = {}
        self.wind_abbr_cache = {}
        self.mongo_cache = {}
        
        # 批量操作缓存
        self.batch_insert_queue = []
        self.batch_update_queue = []
        self.batch_size = 100
        
    @lru_cache(maxsize=1000)
    def get_norm_company_info(self, company_name):
        """缓存norm company信息查询"""
        if company_name not in self.norm_company_cache:
            self.norm_company_cache[company_name] = search_documents("company_norm", "zh_company_name", company_name)
        return self.norm_company_cache[company_name]
    
    @lru_cache(maxsize=1000)
    def get_wind_abbr(self, company_name):
        """缓存wind abbreviation查询"""
        if company_name not in self.wind_abbr_cache:
            try:
                result = wind_search(f"select PREVIOUS_COMP_NAME from ASHAREINTRODUCTIONE_EXT_DF where COMP_NAME = '{company_name}'")
                self.wind_abbr_cache[company_name] = result[0][0].split(",") if result else []
            except Exception as e:
                logger.warning(f"Wind查询失败 {company_name}: {e}")
                self.wind_abbr_cache[company_name] = []
        return self.wind_abbr_cache[company_name]
    
    def get_company_info_optimized(self, norm_company_infos, parent_com, stock_codes):
        """优化的公司信息获取"""
        base_infos = {}
        # 使用字典和集合提高查找效率
        stock_code_set = set(stock_codes)
        
        for norm_company_info in norm_company_infos:
            code = norm_company_info.get('stock_code')
            if code in stock_code_set and norm_company_info.get('zh_company_name') == parent_com:
                if code not in base_infos:
                    base_infos[code] = norm_company_info
                    
        return base_infos
    
    def process_abbreviations(self, norm_company_infos, parent_com, subs_orig, stock_codes):
        """优化的简称处理逻辑"""
        # 使用集合提高查找和去重效率
        abbrs = set()
        
        # 添加现有简称
        for norm_company_info in norm_company_infos:
            search_name = norm_company_info.get('search_company_name')
            if search_name:
                abbrs.add(search_name)
        
        # 添加子公司原始名称
        if subs_orig and subs_orig != parent_com:
            abbrs.add(subs_orig)
        
        # 添加wind简称
        wind_abbrs = self.get_wind_abbr(parent_com)
        abbrs.update(wind_abbrs)
        
        # 移除股票代码和母公司名称
        abbrs.discard(parent_com)
        for code in stock_codes:
            abbrs.discard(code)
            
        return list(abbrs)
    
    def batch_mongo_search(self, abbrs):
        """批量MongoDB查询"""
        batch_results = {}
        
        # 使用线程池并发查询
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_abbr = {
                executor.submit(search_company_data, abbr, "company_name"): abbr 
                for abbr in abbrs
            }
            
            for future in as_completed(future_to_abbr):
                abbr = future_to_abbr[future]
                try:
                    num, temp_info = future.result()
                    batch_results[abbr] = (num, temp_info)
                except Exception as e:
                    logger.error(f"MongoDB查询失败 {abbr}: {e}")
                    batch_results[abbr] = (0, [])
                    
        return batch_results
    
    def create_base_record(self, abbr, parent_com, code, base_info):
        """创建基础记录"""
        base_info_copy = base_info.copy()
        base_info_copy.pop("search_company_name", None)
        
        base = {
            "company_name": abbr,
            "norm_company": parent_com,
            "stock_code": code,
            "is_listed": True,
            "exchange": base_info.get("exchange_name", "")
        }
        
        parent_info = base_info_copy.copy()
        parent_info.update({
            'parent_company': base_info['zh_company_name'],
            'parencheck_norm_is_listed': True,
            'parent_exchange': base_info.get("exchange_name", ""),
            'parent_stock_code': base_info['stock_code'],
            'ownership_percentage': "100.00%"
        })
        
        base["parent_companies"] = [parent_info]
        return base
    
    def update_parent_info(self, base, base_info, parent_com):
        """更新父公司信息"""
        base_info_copy = base_info.copy()
        base_info_copy.pop("search_company_name", None)
        
        parent_companies = base.get('parent_companies', [])
        
        if len(parent_companies) == 1:
            parent_info = base_info_copy.copy()
            parent_info.update({
                'parent_company': base_info['zh_company_name'],
                'parencheck_norm_is_listed': True,
                'parent_exchange': base_info.get("exchange_name", ""),
                'parent_stock_code': base_info['stock_code'],
                'ownership_percentage': "100.00%"
            })
            base["parent_companies"] = [parent_info]
        else:
            for parent_company in parent_companies:
                if parent_company.get('parent_company') == parent_com:
                    parent_company.update({
                        'parent_company': base_info['zh_company_name'],
                        'parencheck_norm_is_listed': True,
                        'parent_exchange': base_info.get("exchange_name", ""),
                        'parent_stock_code': base_info['stock_code'],
                        'ownership_percentage': "100.00%"
                    })
                    break
            base["parent_companies"] = parent_companies
        
        return base
    
    def flush_batch_operations(self):
        """执行批量操作"""
        if self.batch_insert_queue:
            logger.info(f"批量插入 {len(self.batch_insert_queue)} 条记录")
            for record in self.batch_insert_queue:
                try:
                    insert_data("company", record)
                except Exception as e:
                    logger.error(f"批量插入失败: {e}")
            self.batch_insert_queue.clear()
            
        if self.batch_update_queue:
            logger.info(f"批量更新 {len(self.batch_update_queue)} 条记录")
            for record_id, record in self.batch_update_queue:
                try:
                    upsert_data("company", record_id, record)
                except Exception as e:
                    logger.error(f"批量更新失败: {e}")
            self.batch_update_queue.clear()
    
    def process_companies(self, company_data, start_index=0):
        """主处理函数"""
        processed_count = 0
        
        for i, (parent_com, parent_abbr, subs_orig) in enumerate(tqdm(company_data[start_index:], desc="处理公司")):
            current_index = i + start_index
            logger.info(f'====================={current_index}=============================')
            logger.info(f"处理公司: {parent_com}, 简称: {parent_abbr}, 子公司: {subs_orig}")
            
            # 获取规范化公司信息
            norm_company_infos = self.get_norm_company_info(parent_com)
            
            if not norm_company_infos:
                logger.warning(f"未找到规范化信息: {parent_com}")
                continue
                
            # 提取股票代码
            stock_codes = list(set([info['stock_code'] for info in norm_company_infos]))
            
            # 处理简称
            abbrs = self.process_abbreviations(norm_company_infos, parent_com, subs_orig, stock_codes)
            
            # 获取基础信息
            base_infos = self.get_company_info_optimized(norm_company_infos, parent_com, stock_codes)
            
            logger.info(f"股票代码: {stock_codes}")
            logger.info(f"公司简称列表: {abbrs}")
            
            if not abbrs:
                logger.info("没有需要处理的简称")
                continue
            
            # 批量查询MongoDB
            mongo_results = self.batch_mongo_search(abbrs)
            
            # 处理每个简称
            for abbr in abbrs:
                num, temp_info = mongo_results.get(abbr, (0, []))
                
                if num == 0:
                    # 新增记录
                    logger.info(f"新增公司记录: {abbr}")
                    for code in stock_codes:
                        base_info = base_infos.get(code)
                        if base_info:
                            try:
                                record = self.create_base_record(abbr, parent_com, code, base_info)
                                self.batch_insert_queue.append(record)
                                
                                # 检查是否需要执行批量操作
                                if len(self.batch_insert_queue) >= self.batch_size:
                                    self.flush_batch_operations()
                                    
                            except Exception as e:
                                logger.error(f"创建记录失败 {abbr}: {e}")
                                
                elif num > 0:
                    # 更新记录
                    base = temp_info[0]
                    try:
                        stock_code = base.get('stock_code')
                        base_info = base_infos.get(stock_code)
                        
                        if base_info:
                            updated_base = self.update_parent_info(base, base_info, parent_com)
                            self.batch_update_queue.append((base['_id'], updated_base))
                            
                            # 检查是否需要执行批量操作
                            if len(self.batch_update_queue) >= self.batch_size:
                                self.flush_batch_operations()
                                
                    except Exception as e:
                        logger.error(f"更新记录失败 {abbr}: {e}")
            
            processed_count += 1
            
            # 定期执行批量操作
            if processed_count % 50 == 0:
                self.flush_batch_operations()
                logger.info(f"已处理 {processed_count} 个公司")
        
        # 处理完成后执行剩余的批量操作
        self.flush_batch_operations()
        logger.info(f"总共处理了 {processed_count} 个公司")

def main():
    """主函数"""
    start_time = time.time()
    
    # 初始化处理器
    processor = CompanyProcessor()
    
    # 执行SQL查询
    subidiary_search = """
    WITH C AS (SELECT a.secu,a.ticker, a.rpt, s.std_sch as subs_orig, a.subs_cat,a.subs_id, a.reg_std, a.bizzplace_std,a.directrate ,a.indirectrate ,a.totalrate
    FROM equity_subsidiary_base a
    JOIN std_org s ON a.subs_id = s.ref_company_id and a.subs_orig != s.std_sch and a.subs_cat = 2)
    SELECT DISTINCT b.org as tar_name,b.abbr,C.subs_orig
    FROM C
    JOIN base_stock b ON b.code = C.secu
    """
    
    logger.info("开始查询子公司数据...")
    res = chinascope_search(subidiary_search)
    logger.info(f"查询到 {len(res)} 条子公司数据")
    
    if res:
        logger.debug(f"第一条数据示例: {res[0]}")
        
        # 开始处理
        start_index = 28477
        processor.process_companies(res, start_index)
    
    end_time = time.time()
    logger.info(f"总执行时间: {end_time - start_time:.2f} 秒")

if __name__ == "__main__":
    main() 