from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import os
import sys
from pathlib import Path

add_path = str(Path(__file__).parent.parent)
sys.path.append(add_path)
os.chdir(add_path)
from configs.config import elasticsearch

# es = Elasticsearch([{"host":"10.100.0.2","port":9200,"scheme":"http"}],http_auth=("elastic","v_iQ-pOXwvsf1VEw2onc"))
es = Elasticsearch([{"host":elasticsearch.get("ip"),"port":elasticsearch.get("port"),"scheme":"http"}],
                   http_auth=(elasticsearch.get("user"),elasticsearch.get("password")))

def index_documents(index_name,documents):
    es.index(index=index_name,body=documents)

def bulk_index_documents(documents,index_name):
    try:
        # 设置 raise_on_error=False 来获取失败的文档列表而不是抛出异常
        success_count, failed_docs = bulk(es, documents, raise_on_error=False)
        print(f"批量写入完成:{index_name} 成功 {success_count} 条, 失败 {len(failed_docs)} 条")
        if failed_docs:
            print('失败的文档:')
            for item in failed_docs:
                print(f"错误详情: {item}")
                # 如果需要更详细的错误信息，可以打印特定字段
                if 'error' in item:
                    print(f"  - 错误类型: {item.get('error', {}).get('type', 'Unknown')}")
                    print(f"  - 错误原因: {item.get('error', {}).get('reason', 'Unknown')}")
        return success_count, failed_docs
    except Exception as e:
        print(f"批量索引过程中发生异常: {e}")
        return 0, []


def search_documents(index_name, field,company_name):
    """
    在company_norm索引中精确查询company字段
    返回company字段与输入完全匹配的结果
    """
    # 首先尝试使用keyword字段精确匹配
    query_keyword = {
        "term": {
            f"{field}.keyword": company_name
        }
    }
    
    # 备用查询：使用match_phrase进行精确短语匹配
    query_phrase = {
        "match_phrase": {
            f"{field}": company_name
        }
    }
    
    try:
        # 先尝试keyword查询
        res = es.search(index=index_name, body={"query": query_phrase})
        total_num = res['hits']['total']['value']
        if total_num > 0:
            return [item['_source'] for item in res['hits']['hits']]
        else:
            return {}
    except Exception as e:
        print(f"搜索错误: {e}")
        return {}

def search_company_exact(company_name):
    """
    专门用于在company_norm索引中精确查询公司名称的便捷函数
    """
    return search_documents("company_norm", company_name)

def search_company_fuzzy(index_name, company_name):
    """
    模糊匹配搜索公司名称
    """
    query = {
        "multi_match": {
            "query": company_name,
            "fields": ["company", "norm_company"],
            "type": "best_fields",
            "fuzziness": "AUTO"
        }
    }
    
    try:
        res = es.search(index=index_name, body={"query": query})
        
        for hit in res['hits']['hits']:
            source = hit["_source"]
            score = hit["_score"]
            print(f"Score: {score}, Company: {source.get('company')}, Norm Company: {source.get('norm_company')}")
        
        return res
    except Exception as e:
        print(f"模糊搜索错误: {e}")
        return None

# print(search_documents("company_norm","zh_company_name","上海机电股份有限公司"))