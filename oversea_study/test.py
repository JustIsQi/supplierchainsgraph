

from prompt import OVERSEA_STUDY_PROMPT,output_schema
import json
from openai import OpenAI
import os
from pathlib import Path
import sys

add_path = str(Path(__file__).parent.parent)
sys.path.append(add_path)
os.chdir(add_path)

from utils.data_prepare import read_single_md_file


def qwen_chat(message):  
    client = OpenAI(
        api_key="sk-1234",
        base_url="http://10.100.0.205:4000",

        # base_url="http://127.0.0.1:30000/v1",
        # api_key="EMPTY",
        
    )
   
    completion = client.chat.completions.parse(
        model="Qwen3-30B-A3B-Thinking-2507",#Qwen3-30B-A3B-Instruct-2507  
        # model= "Qwen3-Next-80B-A3B-Thinking",
        messages=[
            # {"role": "system", "content": "严格遵循：仅从提供的文档中抽取信息；禁止编造、禁止任何计算或单位换算；缺失即为null；所有数值保持原文格式（包含小数位数、千分位和单位）。输出必须符合指定schema。"},
            {"role":"user","content":message}
        ],
        temperature=0.6,  # 降低随机性，提高输出一致性
        top_p=0.95,        # 降低采样范围，减少胡乱生成
        # presence_penalty=0.5,  # 移除惩罚项，避免干扰信息提取
        extra_body={"top_k":20,"min_p":0.0},  # 减少候选词数量
        response_format={
            "type": "json_object"
        },
        timeout=3600
    )
    
    return completion.choices[0].message.content

# 设置目录路径
datasets_dir = Path("/data/share2/yy/workspace/code/supplierchainsgraph/oversea_study/datasets")
results_dir = Path("/data/share2/yy/workspace/code/supplierchainsgraph/oversea_study/results")

# 创建 results 文件夹（如果不存在）
results_dir.mkdir(parents=True, exist_ok=True)

# 遍历 datasets 目录下的所有文件
md_files = list(datasets_dir.glob("*.md"))
total_files = len(md_files)

print(f"找到 {total_files} 个文件需要处理")

for idx, md_file in enumerate(md_files, 1):
    print(f"\n[{idx}/{total_files}] 正在处理: {md_file.name}")
    
    try:
        # 读取文件内容
        md_content = read_single_md_file(str(md_file))
        
        # 构建 prompt
        prompt = OVERSEA_STUDY_PROMPT.format(output_schema=output_schema, documents=md_content)
        
        # 调用模型推理
        response = qwen_chat(prompt)
        
        # 解析 JSON 响应
        result_json = json.loads(response)
        
        # 生成输出文件名（将 .md 替换为 .json）
        output_filename = md_file.stem + ".json"
        output_path = results_dir / output_filename
        
        # 保存结果到 JSON 文件
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(result_json, f, ensure_ascii=False, indent=2)
        
        print(f"✓ 成功保存到: {output_path}")
        
    except Exception as e:
        print(f"✗ 处理失败: {md_file.name}")
        print(f"  错误信息: {str(e)}")
        continue

print(f"\n处理完成！共处理 {total_files} 个文件，结果保存在: {results_dir}")