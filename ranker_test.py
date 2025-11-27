from utils.split_markdown_by_headers import split_by_headers
from utils.data_prepare import read_single_md_file
import requests
import pandas as pd

path = './windanno_3935be77-7892-58ab-ae60-c15dcbcfca33.md'
report_labels = pd.read_excel("data/report.xlsx", engine='openpyxl').fillna('').values.tolist()
report_labels = [item[0]+' '+item[1] for item in report_labels]
md_content = read_single_md_file(path)
sections = split_by_headers(md_content)

for section in sections:
        header = section['title']
        content = section['content']

        # 过滤掉空的 context，避免 400 错误
        datas = [{"question": "上市公司年报章节内容匹配"+header, "context": item} 
                 for item in report_labels 
                 if item and str(item).strip()]  # 过滤空值和空字符串

        if not datas:
            print(f"⚠️ 章节 {header} 没有有效数据，跳过")
            continue

        try:
            response = requests.post(
                "http://10.100.0.1:7004/rerank/batch",
                json=datas,
                headers={"Content-Type": "application/json"}
            )
            
            # 如果请求失败，打印详细错误信息
            if response.status_code != 200:
                print(f"❌ 请求失败，状态码: {response.status_code}")
                print(f"错误响应: {response.text}")
                print(f"请求数据数量: {len(datas)}")
                print(f"第一条数据示例: {datas[0] if datas else 'N/A'}")
                response.raise_for_status()
            
            response.raise_for_status()
            response_data = response.json()
            
            for data in response_data:
                print(data)
                pruned_context = data['pruned_context']
                score = data['reranking_score']
                print(pruned_context,score)
                print('-'*100)
        except requests.exceptions.HTTPError as e:
            print(f"❌ HTTP 错误: {e}")
            if hasattr(e.response, 'text'):
                print(f"错误详情: {e.response.text}")
            continue
        except Exception as e:
            print(f"❌ 其他错误: {e}")
            continue