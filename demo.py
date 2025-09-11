from pathlib import Path
from utils.data_prepare import read_single_md_file
import os
import json

root = Path('./wind_anno')          # 把这里换成你要遍历的目录

all_files = [p for p in root.rglob('*') if p.is_file()]

ignores = ["industry","business_scope","business_type","share_type","company_qualification"]
total,success = 0,0
for file_path in all_files:
    file_name = file_path.stem
    response_path = f'responses/{file_name}_response.json'
   
    if os.path.exists(response_path):
        md_content = read_single_md_file(file_path)
        with open(response_path,'r',encoding='utf-8') as fp:
            response = json.load(fp)
        for key,value in response.items():
            if isinstance(value,dict):
                for k,v in value.items():
                    if v and isinstance(v,str) and k not in ignores:
                        total += 1
                        if v in md_content:
                            success += 1
                        else:
                            print(f'{file_name} {key} {k} {v} not in md_content')
            elif isinstance(value,list):
                for i,item in enumerate(value):
                    for k_,info in item.items():
                        if info and isinstance(info,str) and k_ not in ignores:
                            total += 1
                            if info in md_content:
                                success += 1
                            else:
                                print(f'{file_name} {key} {k_} {info} not in md_content')
print(f'total: {total}, success: {success}, success rate: {success/total}')