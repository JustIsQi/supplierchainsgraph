import os
import sys
from pathlib import Path
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('company_norm_processing.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

add_path = str(Path(__file__).parent.parent)
sys.path.append(add_path)
os.chdir(add_path)

from utils.es import search_documents
from utils.mysql_util import chinascope_search,wind_search
from utils.mongo import search_company_data,insert_data,upsert_data
from tqdm import tqdm

def get_company_info(norm_company_infos,parent_com,stock_codes):
    base_infos = {}
    for code in stock_codes:
        for norm_company_info in norm_company_infos:
            if norm_company_info['stock_code'] == code and norm_company_info['zh_company_name'] == parent_com:
                if not base_infos.get(code,None):
                    base_infos[code] = norm_company_info
    return base_infos

subidiary_search = """
WITH C AS (SELECT a.secu,a.ticker, a.rpt, s.std_sch as subs_orig, a.subs_cat,a.subs_id, a.reg_std, a.bizzplace_std,a.directrate ,a.indirectrate ,a.totalrate
FROM equity_subsidiary_base a
JOIN std_org s ON a.subs_id = s.ref_company_id and a.subs_orig != s.std_sch and a.subs_cat = 2)
SELECT DISTINCT b.org as tar_name,b.abbr,C.subs_orig
FROM C
JOIN base_stock b ON b.code = C.secu
"""
res = chinascope_search(subidiary_search)
logger.info(f"查询到 {len(res)} 条子公司数据")
logger.debug(f"第一条数据示例: {res[0]}")
# logger.debug(search_documents("company_norm", "zh_company_name", "三力士股份有限公司"))

start_index = 0
for i, (parent_com,parent_abbr,subs_orig) in enumerate(tqdm(res[start_index:])):
    logger.info(f'====================={i+start_index }=============================')
    logger.info(f"处理公司: {parent_com}, 简称: {parent_abbr}, 子公司: {subs_orig}")
    norm_company_infos= search_documents("company_norm", "zh_company_name", parent_com)
    if norm_company_infos:
        stock_codes = set([norm_company_info['stock_code'] for norm_company_info in norm_company_infos])
        abbrs = list(set([norm_company_info['search_company_name'] for norm_company_info in norm_company_infos]))

        # 添加subs_orig到处理列表
        if subs_orig and subs_orig not in abbrs and subs_orig != parent_com:
            abbrs.append(subs_orig)

        try:
            wind_abbr = wind_search(f"select PREVIOUS_COMP_NAME  from ASHAREINTRODUCTIONE_EXT_DF where COMP_NAME = '{parent_com}'")[0][0]
            abbrs.extend(wind_abbr.split(","))
        except Exception as e:
            pass
       
        for code in stock_codes:
            if code in abbrs:
                abbrs.remove(code)
        abbrs[:] = [x for x in abbrs if x != parent_com]
       
        base_infos = get_company_info(norm_company_infos,parent_com,stock_codes)                                                                                                   
        logger.info(f"股票代码: {stock_codes}")
        logger.info(f"公司简称列表: {abbrs}")
        logger.info(f"基础信息: {base_infos}")

        for abbr in abbrs:
            num,temp_info = search_company_data(abbr,"company_name")
            if num == 0:
                logger.info(f"新增公司记录: {abbr}")
                for code in stock_codes:
                    try:
                        base_info = base_infos.get(code,None)
                        base_info.pop("search_company_name",None)

                        base = {"company_name":abbr,"norm_company":parent_com,"stock_code":code,"is_listed":True,"exchange":base_info.get("exchange_name","")}
                        base_info['parent_company'] = base_info['zh_company_name']
                        base_info['parencheck_norm_is_listed'] = True
                        base_info['parent_exchange'] = base_info.get("exchange_name","")
                        base_info['parent_stock_code'] = base_info['stock_code']
                        base_info['ownership_percentage'] = "100.00%"
                        base["parent_companies"] = [base_info]
                        insert_data("company",base)
                    except:
                        pass
                    
            elif num > 0:
                base = temp_info[0]
                # logger.info(f"待更新公司记录: {base}")
                try:
                    base_info = base_infos.get(base['stock_code'],None)
                    base_info.pop("search_company_name",None)

                    parent_companies = base['parent_companies'] 
                    if len(parent_companies) == 1:
                        base_info['parent_company'] = base_info['zh_company_name']
                        base_info['parencheck_norm_is_listed'] = True
                        base_info['parent_exchange'] = base_info.get("exchange_name","")
                        base_info['parent_stock_code'] = base_info['stock_code']
                        base_info['ownership_percentage'] = "100.00%"
                        base["parent_companies"] = [base_info]
                        
                    else:
                        for parent_company in parent_companies:
                            if parent_company['parent_company'] == parent_com:
                                parent_company['parent_company'] = base_info['zh_company_name']
                                parent_company['parencheck_norm_is_listed'] = True
                                parent_company['parent_exchange'] = base_info.get("exchange_name","")
                                parent_company['parent_stock_code'] = base_info['stock_code']
                                parent_company['ownership_percentage'] = "100.00%"
                                break

                        base["parent_companies"] = parent_companies
                    upsert_data("company",base['_id'],base)
                except Exception as e:
                    logger.error(f"更新公司记录时发生错误: {e},mongo data:{base}")


