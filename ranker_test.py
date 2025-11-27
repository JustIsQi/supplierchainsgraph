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

        datas = [{"question": "上市公司年报章节内容匹配"+header,"documents": item} for index,item in enumerate(report_labels)]

        response = requests.post(
            "http://10.100.0.1:7004/rerank",
            json=datas,
            headers={"Content-Type": "application/json"}
        )
        response_data = response.json()
        for data in response_data:
            pruned_context = data['pruned_context']
            score = data['reranking_score']
            print(pruned_context,score)
            print('-'*100)