

from prompt import OVERSEA_STUDY_PROMPT,output_schema
import json
from openai import OpenAI
import os
from pathlib import Path
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

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


def gpt_oss_chat(message):
    client = OpenAI(
        api_key="EMPTY",  # vLLM 部署通常不需要真实 API key
        base_url="http://10.100.0.2:8002/v1",  # vLLM 默认端口，请根据实际部署情况修改
    )
    
    completion = client.chat.completions.create(
        model="gptoss",
        messages=[
            {"role": "user", "content": message}
        ],
        temperature=0.6,
        top_p=0.95,
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

# 定义模型配置
models = [
    {"name": "qwen", "func": qwen_chat, "display_name": "Qwen3-30B"},
    {"name": "gpt-oss", "func": gpt_oss_chat, "display_name": "GPT-OSS-20B"}
]

for idx, md_file in enumerate(md_files, 1):
    print(f"\n[{idx}/{total_files}] 正在处理: {md_file.name}")
    
    # 读取文件内容（所有模型共用）
    try:
        md_content = read_single_md_file(str(md_file))
        prompt = OVERSEA_STUDY_PROMPT.format(output_schema=output_schema, documents=md_content)
    except Exception as e:
        print(f"✗ 读取文件失败: {md_file.name}")
        print(f"  错误信息: {str(e)}")
        continue
    
    # 定义单个模型推理任务
    def process_with_model(model_config, prompt, md_file, results_dir):
        model_name = model_config["name"]
        model_func = model_config["func"]
        display_name = model_config["display_name"]
        
        try:
            print(f"  → 使用 {display_name} 推理中...")
            
            # 调用模型推理
            response = model_func(prompt)
            
            # 解析 JSON 响应
            result_json = json.loads(response)
            
            # 生成输出文件名（包含模型名称）
            output_filename = f"{md_file.stem}_{model_name}.json"
            output_path = results_dir / output_filename
            
            # 保存结果到 JSON 文件
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(result_json, f, ensure_ascii=False, indent=2)
            
            return {"success": True, "display_name": display_name, "output_file": output_path.name}
            
        except Exception as e:
            return {"success": False, "display_name": display_name, "error": str(e)}
    
    # 使用多线程同时推理两个模型
    with ThreadPoolExecutor(max_workers=len(models)) as executor:
        # 提交所有模型推理任务
        futures = {
            executor.submit(process_with_model, model, prompt, md_file, results_dir): model
            for model in models
        }
        
        # 收集结果
        for future in as_completed(futures):
            result = future.result()
            if result["success"]:
                print(f"  ✓ {result['display_name']} 成功保存到: {result['output_file']}")
            else:
                print(f"  ✗ {result['display_name']} 处理失败")
                print(f"    错误信息: {result['error']}")

print(f"\n处理完成！共处理 {total_files} 个文件，使用 {len(models)} 个模型，结果保存在: {results_dir}")