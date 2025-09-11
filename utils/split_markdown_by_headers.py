#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Markdown文档按二级、三级标题拆分工具
"""

import re
import os
from typing import List, Dict, Tuple

def read_markdown_file(file_path: str) -> str:
    """读取markdown文件内容"""
    with open(file_path, 'r', encoding='utf-8') as f:
        return f.read()

def split_by_headers(content: str) -> List[Dict[str, str]]:
    """
    按照二级和三级标题拆分markdown内容
    
    Args:
        content: markdown文件内容
        
    Returns:
        List[Dict]: 包含标题和内容的字典列表
    """
    # 分割线，用于按行处理
    lines = content.split('\n')
    
    sections = []
    current_section = {
        'level': 0,
        'title': '',
        'content': []
    }

    for line in lines:
        # 检查是否是二级标题
        if line.startswith('## '):
            # 如果当前有内容，保存当前节
            if current_section['content'] or current_section['title']:
                if current_section['title'] or any(current_section['content']):
                    sections.append({
                        'level': current_section['level'],
                        'title': current_section['title'],
                        'content': '\n'.join(current_section['content']).strip()
                    })
            
            # 开始新的二级标题节
            current_section = {
                'level': 2,
                'title': line,
                'content': []
            }
        
        # 检查是否是三级标题
        elif line.startswith('### '):
            # 如果当前有内容，保存当前节
            if current_section['content'] or current_section['title']:
                if current_section['title'] or any(current_section['content']):
                    sections.append({
                        'level': current_section['level'],
                        'title': current_section['title'],
                        'content': '\n'.join(current_section['content']).strip()
                    })
            
            # 开始新的三级标题节
            current_section = {
                'level': 3,
                'title': line,
                'content': []
            }
        
        else:
            # 将内容行添加到当前节
            current_section['content'].append(line)
    
    # 添加最后一节
    if current_section['title'] or any(current_section['content']):
        sections.append({
            'level': current_section['level'],
            'title': current_section['title'],
            'content': '\n'.join(current_section['content']).strip()
        })
    
    return sections


# def main():
#     # 输入文件路径
#     input_file = "/data/true_nas/zfs_share1/yy/code/supplierchainsgraph/wind_anno/年度报告_windanno_bc68d567-eab0-5fcf-8eb0-10d184f337dc.md"
    
#     # 输出目录
#     output_dir = "/data/true_nas/zfs_share1/yy/code/supplierchainsgraph/split_sections"
#     base_name = "宁德时代年度报告"
    
#     print(f"正在读取文件: {input_file}")
    
#     # 读取markdown文件
#     content = read_markdown_file(input_file)
#     print(f"文件大小: {len(content)} 字符")
    
#     # 按标题拆分
#     sections = split_by_headers(content)

#     for section in sections:
#         print(section['title'])
#         print(section['content'])
#         print('-'*100)
    
    
#     print(f"\n拆分完成! 所有段落已保存到: {output_dir}")

# if __name__ == "__main__":
#     main() 