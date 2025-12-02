

from prompt import OVERSEA_STUDY_PROMPT,output_schema
import json
from openai import OpenAI
import os
from pathlib import Path
import sys
import requests
import time

add_path = str(Path(__file__).parent.parent)
sys.path.append(add_path)
os.chdir(add_path)

from utils.data_prepare import read_single_md_file


def split_text_into_paragraphs(text):
    """
    将长文本按 markdown 标题分割成段落
    以 # 开头的为标题，两个标题之间为一个段落
    
    Args:
        text: 输入文本
        max_length: 每个段落的最大字符数（暂时保留参数，未来可用于进一步分割）
    
    Returns:
        list: 段落列表，每个段落是一个字典 {"title": 标题, "content": 内容}
    """
    lines = text.split('\n')
    paragraphs = []
    current_title = ""
    current_content = []
    
    for line in lines:
        # 检查是否是标题行（以一个或多个 # 开头）
        if line.strip().startswith('#'):
            # 保存前一个段落
            if current_title or current_content:
                content_text = '\n'.join(current_content).strip()
                if content_text:  # 只添加有内容的段落
                    paragraphs.append({
                        "title": current_title,
                        "content": content_text
                    })
            
            # 开始新的段落
            current_title = line.strip()
            current_content = []
        else:
            # 累积当前段落的内容
            current_content.append(line)
    
    # 添加最后一个段落
    if current_title or current_content:
        content_text = '\n'.join(current_content).strip()
        if content_text:
            paragraphs.append({
                "title": current_title,
                "content": content_text
            })
    
    return paragraphs


def rerank_paragraphs(paragraphs, query, top_k=10, score_threshold=0.3):
    """
    使用rerank模型对段落进行相关性排序和筛选
    
    Args:
        paragraphs: 段落列表（字典列表，包含 title 和 content）
        query: 查询文本，用于判断段落相关性
        top_k: 返回的最相关段落数量
        score_threshold: 相关性分数阈值，低于此分数的段落将被过滤
    
    Returns:
        list: 筛选后的段落列表（按原文顺序）
    """
    if not paragraphs:
        return []
    
    try:
        # 提取段落内容用于rerank（标题+内容）
        paragraph_texts = []
        for para in paragraphs:
            title = para.get('title', '')
            content = para.get('content', '')
            text = f"{title}\n{content}" if title else content
            paragraph_texts.append(text)
        
        # 调用rerank API
        data = {
            "model": "Bge-ReRanker",
            'query': query,
            'documents': paragraph_texts
        }
        
        headers = {
            "accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer sk-1234",
        }
        
        response = requests.post(
            'http://10.100.0.205:4000/rerank',
            headers=headers,
            json=data,
            timeout=60
        )
        
        if response.status_code != 200:
            print(f"  ⚠ Rerank API 请求失败 (状态码: {response.status_code})，返回所有段落")
            return paragraphs
        
        ranked_results = json.loads(response.text)['results']
        
        # 按分数排序并筛选
        sorted_results = sorted(ranked_results, key=lambda x: x['relevance_score'], reverse=True)
        
        # 如果结果数量不足，直接返回所有结果
        if len(sorted_results) < top_k:
            filtered_results = sorted_results
        else:
            # 筛选出分数高于阈值的结果，但至少返回前 top_k 个
            filtered_results = [
                r for r in sorted_results 
                if r['relevance_score'] >= score_threshold
            ]
            # 如果过滤后结果太少，至少保留前 top_k 个（即使分数低于阈值）
            if len(filtered_results) < top_k:
                filtered_results = sorted_results[:top_k]
            else:
                filtered_results = filtered_results[:top_k]
        
        # 按原文顺序重新排列（保持文档的逻辑顺序）
        filtered_results = sorted(filtered_results, key=lambda x: x['index'])
        
        # 提取段落
        selected_paragraphs = [paragraphs[r['index']] for r in filtered_results]
        
        # for para in selected_paragraphs:
        #     print(para['title'])
        #     print(para['content'])
        #     print('--------------------------------\n\n')

        print(f"  ✓ 段落级Rerank完成: {len(paragraphs)} 段落 → {len(selected_paragraphs)} 段落 (阈值: {score_threshold}, top_k: {top_k})")
        if filtered_results:
            print(f"  ✓ 分数范围: {filtered_results[0]['relevance_score']:.3f} ~ {filtered_results[-1]['relevance_score']:.3f}")
            print(f"  ✓ 保留率: {len(selected_paragraphs)/len(paragraphs)*100:.1f}%")
        
        return selected_paragraphs
        
    except Exception as e:
        print(f"  ⚠ Rerank处理失败: {str(e)}，返回所有段落")
        return paragraphs


def preprocess_document(md_content, enable_rerank=True):
    """
    预处理文档：段落分割和精排
    1. 按 markdown 标题分割段落
    2. 段落级别：使用rerank筛选相关段落
    
    Args:
        md_content: markdown文档内容
        enable_rerank: 是否启用rerank筛选
    
    Returns:
        str: 筛选后的文档内容
    """
    # 第一步：按标题分割段落
    paragraphs = split_text_into_paragraphs(md_content)
    print(f"  ✓ 文档分割: {len(md_content)} 字符 → {len(paragraphs)} 段落")
    
    if not enable_rerank or len(paragraphs) <= 5:
        # 如果段落数量较少，不需要rerank，直接返回
        result_paragraphs = []
        for para in paragraphs:
            title = para.get('title', '')
            content = para.get('content', '')
            if title:
                result_paragraphs.append(f"{title}\n{content}")
            else:
                result_paragraphs.append(content)
        return '\n\n'.join(result_paragraphs)
    
    # 第二步：段落级别的rerank筛选 - 聚焦产品、技术、营收核心业务
    rank_query = "Core business disclosure: product lines and services, technology and R&D capabilities, revenue and sales by segment. Product portfolio, technology innovation, revenue breakdown by product category or geographic market."
    selected_paragraphs = rerank_paragraphs(
        paragraphs,
        query=rank_query,
        top_k=50,  # 选择最相关的段落
        score_threshold=0.1  # 分数阈值
    )
    
    # 第三步：重新组装段落
    result_paragraphs = []
    for para in selected_paragraphs:
        title = para.get('title', '')
        content = para.get('content', '')
        if title:
            result_paragraphs.append(f"{title}\n{content}")
        else:
            result_paragraphs.append(content)
    
    filtered_content = "\n\n".join(result_paragraphs)
    print(f"  ✓ 内容压缩: {len(md_content)} 字符 → {len(filtered_content)} 字符 (压缩率: {(1-len(filtered_content)/len(md_content))*100:.1f}%)")
    
    return filtered_content


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
results_dir = Path("/data/share2/yy/workspace/code/supplierchainsgraph/oversea_study/datasets")

# 创建 results 文件夹（如果不存在）
results_dir.mkdir(parents=True, exist_ok=True)

# 遍历 datasets 目录下的所有文件
md_files = list(datasets_dir.glob("*.md"))
total_files = len(md_files)

print(f"找到 {total_files} 个文件需要处理")

for idx, md_file in enumerate(md_files[:2], 1):
    print(f"\n[{idx}/{total_files}] 正在处理: {md_file.name}")
    
    # 读取文件内容（所有模型共用）
    try:
        md_content = read_single_md_file(str(md_file))
        print(f"  ✓ 原始文档: {len(md_content)} 字符")
        
        # 使用rerank预处理文档，筛选相关段落
        filtered_content = preprocess_document(md_content, enable_rerank=True)
        # print('\n\n',filtered_content,'\n\n')
        # 构建prompt - 使用字符串替换避免花括号冲突
        prompt = OVERSEA_STUDY_PROMPT.replace("{output_schema}", output_schema).replace("{documents}", filtered_content)
    except Exception as e:
        print(f"  ✗ 读取/预处理文件失败: {md_file.name}")
        print(f"  错误信息: {str(e)}")
        continue
    
    # 使用 GPT-OSS 模型推理
    try:
        print(f"  → 使用 GPT-OSS-20B 推理中...")
        
        # 记录开始时间
        start_time = time.time()
        
        # 调用模型推理
        response = gpt_oss_chat(prompt)
        
        # 计算执行时间
        elapsed_time = time.time() - start_time
        print(f"  ✓ 模型执行时间: {elapsed_time:.2f} 秒 ({elapsed_time/60:.2f} 分钟)")
        
        # 解析 JSON 响应
        result_json = json.loads(response)
        
        # 生成输出文件名
        output_filename = f"{md_file.stem}_gpt-oss.json"
        output_path = results_dir / output_filename
        
        # 保存结果到 JSON 文件
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(result_json, f, ensure_ascii=False, indent=2)
        
        print(f"  ✓ GPT-OSS-20B 成功保存到: {output_path.name}")
        
    except Exception as e:
        print(f"  ✗ GPT-OSS-20B 处理失败")
        print(f"    错误信息: {str(e)}")

print(f"\n处理完成！共处理 {total_files} 个文件，结果保存在: {results_dir}")