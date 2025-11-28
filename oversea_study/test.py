

from prompt import OVERSEA_STUDY_PROMPT,output_schema
import json
from openai import OpenAI
import os
from pathlib import Path
import sys
import requests
import re

add_path = str(Path(__file__).parent.parent)
sys.path.append(add_path)
os.chdir(add_path)

from utils.data_prepare import read_single_md_file


def split_text_into_paragraphs(text, max_length=3000):
    """
    将长文本分割成段落
    优先按照自然段落分割，如果段落过长则按照句子分割
    
    Args:
        text: 输入文本
        max_length: 每个段落的最大字符数
    
    Returns:
        list: 段落列表
    """
    # 首先按照多个换行符分割自然段落
    paragraphs = re.split(r'\n\n+', text)
    
    result = []
    current_chunk = ""
    
    for para in paragraphs:
        para = para.strip()
        if not para:
            continue
            
        # 如果单个段落就超过最大长度，需要进一步分割
        if len(para) > max_length:
            # 按句子分割（支持中英文）
            sentences = re.split(r'([.!?。！？]\s+)', para)
            temp_chunk = ""
            
            for i in range(0, len(sentences), 2):
                sentence = sentences[i]
                if i + 1 < len(sentences):
                    sentence += sentences[i + 1]
                
                if len(temp_chunk) + len(sentence) > max_length and temp_chunk:
                    result.append(temp_chunk.strip())
                    temp_chunk = sentence
                else:
                    temp_chunk += sentence
            
            if temp_chunk:
                if len(current_chunk) + len(temp_chunk) <= max_length:
                    current_chunk += "\n" + temp_chunk
                else:
                    if current_chunk:
                        result.append(current_chunk.strip())
                    current_chunk = temp_chunk
        else:
            # 如果当前段落加上新段落不超过最大长度，合并
            if len(current_chunk) + len(para) + 2 <= max_length:
                if current_chunk:
                    current_chunk += "\n\n" + para
                else:
                    current_chunk = para
            else:
                # 否则保存当前累积的内容，开始新的块
                if current_chunk:
                    result.append(current_chunk.strip())
                current_chunk = para
    
    # 添加最后一个块
    if current_chunk:
        result.append(current_chunk.strip())
    
    return result


def rerank_paragraphs(paragraphs, query, top_k=10, score_threshold=0.3):
    """
    使用rerank模型对段落进行相关性排序和筛选
    
    Args:
        paragraphs: 段落列表
        query: 查询文本，用于判断段落相关性
        top_k: 返回的最相关段落数量
        score_threshold: 相关性分数阈值，低于此分数的段落将被过滤
    
    Returns:
        list: 筛选后的段落列表（按原文顺序）
    """
    if not paragraphs:
        return []
    
    try:
        # 调用rerank API
        data = {
            "model": "Bge-ReRanker",
            'query': query,
            'documents': paragraphs
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
        
        # 筛选出分数高于阈值的结果
        filtered_results = [
            r for r in sorted_results 
            if r['relevance_score'] >= score_threshold
        ][:top_k]
        
        # 按原文顺序重新排列（保持文档的逻辑顺序）
        filtered_results = sorted(filtered_results, key=lambda x: x['index'])
        
        # 提取段落文本
        selected_paragraphs = [paragraphs[r['index']] for r in filtered_results]
        
        print(f"  ✓ Rerank完成: {len(paragraphs)} 段落 → {len(selected_paragraphs)} 段落 (阈值: {score_threshold})")
        print(f"  ✓ 分数范围: {filtered_results[0]['relevance_score']:.3f} ~ {filtered_results[-1]['relevance_score']:.3f}" if filtered_results else "")
        
        return selected_paragraphs
        
    except Exception as e:
        print(f"  ⚠ Rerank处理失败: {str(e)}，返回所有段落")
        return paragraphs


def preprocess_document(md_content, enable_rerank=True):
    """
    预处理文档：分割段落并使用rerank筛选相关内容
    
    Args:
        md_content: markdown文档内容
        enable_rerank: 是否启用rerank筛选
    
    Returns:
        str: 筛选后的文档内容
    """
    # 分割段落
    paragraphs = split_text_into_paragraphs(md_content, max_length=3000)
    print(f"  ✓ 文档分割: {len(md_content)} 字符 → {len(paragraphs)} 段落")
    
    if not enable_rerank or len(paragraphs) <= 10:
        # 如果段落数量较少，不需要rerank
        return md_content
    
    # 使用rerank筛选相关段落
    query = "Extract production operations revenue financial data business segments from financial report"
    selected_paragraphs = rerank_paragraphs(
        paragraphs,
        query=query,
        top_k=50,  # 选择最相关的15个段落
        score_threshold=0.6  # 分数阈值
    )
    
    # 合并筛选后的段落
    filtered_content = "\n\n".join(selected_paragraphs)
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
results_dir = Path("/data/share2/yy/workspace/code/supplierchainsgraph/oversea_study/results")

# 创建 results 文件夹（如果不存在）
results_dir.mkdir(parents=True, exist_ok=True)

# 遍历 datasets 目录下的所有文件
md_files = list(datasets_dir.glob("*.md"))
total_files = len(md_files)

print(f"找到 {total_files} 个文件需要处理")

for idx, md_file in enumerate(md_files, 1):
    print(f"\n[{idx}/{total_files}] 正在处理: {md_file.name}")
    
    # 读取文件内容（所有模型共用）
    try:
        md_content = read_single_md_file(str(md_file))
        print(f"  ✓ 原始文档: {len(md_content)} 字符")
        
        # 使用rerank预处理文档，筛选相关段落
        filtered_content = preprocess_document(md_content, enable_rerank=True)
        
        # 构建prompt
        prompt = OVERSEA_STUDY_PROMPT.format(output_schema=output_schema, documents=filtered_content)
    except Exception as e:
        print(f"  ✗ 读取/预处理文件失败: {md_file.name}")
        print(f"  错误信息: {str(e)}")
        continue
    
    # 使用 GPT-OSS 模型推理
    try:
        print(f"  → 使用 GPT-OSS-20B 推理中...")
        
        # 调用模型推理
        response = gpt_oss_chat(prompt)
        
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