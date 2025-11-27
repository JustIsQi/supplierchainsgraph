import os
import sys
from pathlib import Path
from pymongo import MongoClient

# 添加项目根目录到Python路径
add_path = str(Path(__file__).parent.parent)
sys.path.append(add_path)
os.chdir(add_path)

from utils.use_tool import US3Client
from configs.config import mongo_url

client = MongoClient(mongo_url)
db = client["OmniDataCrafter"]
collection = db["filings"]


# ISIN到Ticker的映射
isin_to_ticker = {
    "US0378331005": "AAPL",
    "US5949181045": "MSFT",
    "US02079K3059": "GOOGL",
    "US0231351067": "AMZN",
    "US67066G1040": "NVDA",
    "US30303M1027": "META",
    "US88160R1014": "TSLA",
    "US68389X1054": "ORCL",
    "US8740391003": "TSM",
    "US11135F1012": "AVGO",
    "US0079031078": "AMD",
    "USN070592100": "ASML",
    "US01609W1027": "BABA",
    "US7223041028": "PDD",
    "US47215P1066": "JD",
    "US7707001027": "HOOD",
    "US19260Q1076": "COIN",
    "US22160K1051": "COST",
    "US7170811035": "PFE"
}

symbols = list(isin_to_ticker.keys())


us3_client = US3Client()

# 确保下载目录存在
download_dir = Path(__file__).parent / "datasets"
download_dir.mkdir(exist_ok=True)

# 查询符合条件的文档
query = {
    "isin": {"$in": symbols},
    "FilingDocument.DocumentSummary.FormType": {"$in": ["10-K", "10-Q"]}
}

print(f"开始查询符合条件的文档...")
print(f"查询条件: isin在symbols列表中，且FormType为10-K或10-Q")

# 查询文档
documents = collection.find(query)
documents_list = list(documents)
total_count = len(documents_list)

print(f"找到 {total_count} 条符合条件的文档")

# 下载文件
success_count = 0
fail_count = 0
pdf_success_count = 0
pdf_fail_count = 0

for idx, doc in enumerate(documents_list, 1):
    try:
        # 获取us3_path - 遍历rt_parser字典找到us3_path
        filing_doc = doc.get("FilingDocument", {})
        rt_parser = doc.get("rt_parser", {})
        source_id = doc.get("_id",'')
        us3_path = None
        
        # 遍历rt_parser字典，找到第一个包含us3_path的条目
        if isinstance(rt_parser, dict):
            for key, value in rt_parser.items():
                if isinstance(value, dict) and "us3_path" in value:
                    us3_path = value.get("us3_path",'')
                    break
        
        if not us3_path:
            print(f"[{idx}/{total_count}] 跳过: {source_id} 缺少us3_path字段")
            fail_count += 1
            continue
        
        # 获取文件信息
        form_type = filing_doc.get("DocumentSummary", {}).get("FormType", "unknown")
        isin = doc.get("isin", "unknown")
        ticker = isin_to_ticker.get(isin, "UNKNOWN")
        original_file_name = us3_path.split("/")[-1]
        # 在文件名前加上ticker
        file_name = f"{ticker}_{original_file_name}"
        
        # 构建本地保存路径
        local_file_path = download_dir / file_name
        
        # 如果文件已存在，跳过
        if local_file_path.exists():
            print(f"[{idx}/{total_count}] 跳过（已存在）: {file_name} (Ticker: {ticker}, ISIN: {isin}, FormType: {form_type})")
        else:
            print(f"[{idx}/{total_count}] 下载中: {file_name} (Ticker: {ticker}, ISIN: {isin}, FormType: {form_type})")
            
            # 下载MD文件
            status_code = us3_client.download_file(us3_path, str(local_file_path))
            
            if status_code == 200:
                print(f"[{idx}/{total_count}] ✓ MD下载成功: {file_name}")
                success_count += 1
            else:
                print(f"[{idx}/{total_count}] ✗ MD下载失败: {file_name} (状态码: {status_code})")
                fail_count += 1
                # 删除可能创建的不完整文件
                if local_file_path.exists():
                    local_file_path.unlink()
        
        # 下载对应的PDF文件
        # 将us3_path和local_file_path的后缀从.md改为.pdf
        pdf_us3_path = us3_path.replace(".md", ".pdf")
        pdf_file_name = file_name.replace(".md", ".pdf")
        pdf_local_file_path = download_dir / pdf_file_name
        
        # 如果PDF文件已存在，跳过
        if pdf_local_file_path.exists():
            print(f"[{idx}/{total_count}] 跳过（PDF已存在）: {pdf_file_name}")
        else:
            print(f"[{idx}/{total_count}] PDF下载中: {pdf_file_name}")
            
            # 下载PDF文件
            pdf_status_code = us3_client.download_file(pdf_us3_path, str(pdf_local_file_path))
            
            if pdf_status_code == 200:
                print(f"[{idx}/{total_count}] ✓ PDF下载成功: {pdf_file_name}")
                pdf_success_count += 1
            else:
                print(f"[{idx}/{total_count}] ✗ PDF下载失败: {pdf_file_name} (状态码: {pdf_status_code})")
                pdf_fail_count += 1
                # 删除可能创建的不完整文件
                if pdf_local_file_path.exists():
                    pdf_local_file_path.unlink()
                
    except Exception as e:
        print(f"[{idx}/{total_count}] ✗ 处理出错: {str(e)}")
        fail_count += 1

print(f"\n下载完成!")
print(f"总计: {total_count} 条")
print(f"\nMD文件:")
print(f"  成功: {success_count} 条")
print(f"  失败: {fail_count} 条")
print(f"\nPDF文件:")
print(f"  成功: {pdf_success_count} 条")
print(f"  失败: {pdf_fail_count} 条")
print(f"\n下载目录: {download_dir}")