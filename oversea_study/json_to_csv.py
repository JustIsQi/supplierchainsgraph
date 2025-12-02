#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
将 JSON 文件转换为 CSV 和 Excel 格式
处理嵌套的树形结构，提取所有数据节点
"""

import json
import csv
from pathlib import Path
import sys

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    print("警告: pandas 未安装，将无法生成 Excel 文件")
    print("请运行: pip install pandas openpyxl")


def flatten_json_to_rows(data, path="", rows=None):
    """
    递归遍历 JSON 树结构，提取所有数据节点
    
    Args:
        data: 当前节点数据
        path: 当前路径
        rows: 存储所有行的列表
    
    Returns:
        list: 包含所有数据行的列表
    """
    if rows is None:
        rows = []
    
    if not isinstance(data, dict):
        return rows
    
    node_type = data.get("node_type", "")
    description = data.get("description", "")
    
    # 检查是否是数据节点
    if node_type == "data":
        # 这是一个叶子数据节点
        row = {
            "path": path,
            "description": description,
            "value": data.get("value", ""),
            "currency": data.get("currency", ""),
            "unit": data.get("unit", ""),
            "time_period": data.get("time_period", ""),
            "growth_rate": data.get("growth_rate", ""),
            "ratio": data.get("ratio", "")
        }
        rows.append(row)
    
    # 检查是否有 time_periods 子节点
    if "time_periods" in data:
        time_periods = data["time_periods"]
        if isinstance(time_periods, dict) and time_periods.get("node_type") == "category":
            # 遍历所有时间周期
            for time_key, time_data in time_periods.items():
                if time_key != "node_type" and time_key != "description":
                    if isinstance(time_data, dict) and time_data.get("node_type") == "data":
                        row = {
                            "path": path,
                            "description": description,
                            "value": time_data.get("value", ""),
                            "currency": time_data.get("currency", ""),
                            "unit": time_data.get("unit", ""),
                            "time_period": time_data.get("time_period", time_key),
                            "growth_rate": time_data.get("growth_rate", ""),
                            "ratio": time_data.get("ratio", "")
                        }
                        rows.append(row)
    
    # 递归处理所有子节点
    for key, value in data.items():
        if key in ["node_type", "description", "time_periods"]:
            continue
        
        if isinstance(value, dict):
            new_path = f"{path}/{key}" if path else key
            flatten_json_to_rows(value, new_path, rows)
    
    return rows


def json_to_csv(json_file_path, csv_file_path=None):
    """
    将 JSON 文件转换为 CSV 文件
    
    Args:
        json_file_path: JSON 文件路径
        csv_file_path: CSV 输出文件路径（如果为 None，则自动生成）
    """
    json_path = Path(json_file_path)
    
    if not json_path.exists():
        print(f"错误: 文件不存在 {json_file_path}")
        return
    
    # 读取 JSON 文件
    print(f"正在读取: {json_path.name}")
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # 展平数据
    rows = flatten_json_to_rows(data)
    
    if not rows:
        print(f"警告: {json_path.name} 中没有找到数据节点")
        return
    
    # 生成 CSV 文件名
    if csv_file_path is None:
        csv_file_path = json_path.with_suffix('.csv')
    
    csv_path = Path(csv_file_path)
    
    # 过滤掉没有值的空行
    filtered_rows = [row for row in rows if row.get("value") or row.get("time_period")]
    
    # 写入 CSV 文件
    print(f"正在写入: {csv_path.name} ({len(filtered_rows)} 行数据)")
    
    fieldnames = ["path", "description", "time_period", "value", "currency", "unit", "growth_rate", "ratio"]
    
    with open(csv_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(filtered_rows)
    
    print(f"✓ 成功转换: {csv_path.name}\n")


def json_to_excel(json_file_path, excel_file_path=None):
    """
    将 JSON 文件转换为 Excel 文件
    
    Args:
        json_file_path: JSON 文件路径
        excel_file_path: Excel 输出文件路径（如果为 None，则自动生成）
    """
    if not PANDAS_AVAILABLE:
        print("错误: pandas 未安装，无法生成 Excel 文件")
        return
    
    json_path = Path(json_file_path)
    
    if not json_path.exists():
        print(f"错误: 文件不存在 {json_file_path}")
        return
    
    # 读取 JSON 文件
    print(f"正在读取: {json_path.name}")
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # 展平数据
    rows = flatten_json_to_rows(data)
    
    if not rows:
        print(f"警告: {json_path.name} 中没有找到数据节点")
        return
    
    # 过滤掉没有值的空行
    filtered_rows = [row for row in rows if row.get("value") or row.get("time_period")]
    
    if not filtered_rows:
        print(f"警告: {json_path.name} 中没有有效数据")
        return
    
    # 生成 Excel 文件名
    if excel_file_path is None:
        excel_file_path = json_path.with_suffix('.xlsx')
    
    excel_path = Path(excel_file_path)
    
    # 转换为 DataFrame
    df = pd.DataFrame(filtered_rows)
    
    # 重新排列列的顺序
    column_order = ["path", "description", "time_period", "value", "currency", "unit", "growth_rate", "ratio"]
    df = df.reindex(columns=[col for col in column_order if col in df.columns])
    
    # 写入 Excel 文件
    print(f"正在写入: {excel_path.name} ({len(df)} 行数据)")
    
    with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Data', index=False)
        
        # 获取工作表对象以设置列宽
        from openpyxl.utils import get_column_letter
        worksheet = writer.sheets['Data']
        
        # 自动调整列宽
        for idx, col in enumerate(df.columns, start=1):
            max_length = max(
                df[col].astype(str).map(len).max(),
                len(str(col))
            )
            # 设置列宽，最大不超过 50
            column_letter = get_column_letter(idx)
            worksheet.column_dimensions[column_letter].width = min(max_length + 2, 50)
    
    print(f"✓ 成功转换: {excel_path.name}\n")


def main():
    """主函数"""
    # 设置文件路径
    base_dir = Path(__file__).parent / "datasets"
    
    json_files = [
        base_dir / "AMZN_20240202_4295905494_97681637330_1_1_10-K_raw_gpt-oss.json",
        base_dir / "MSFT_20230727_4295907168_97679038724_1_1_10-K_raw_gpt-oss.json"
    ]
    
    print("=" * 60)
    print("JSON 转 Excel/CSV 转换工具")
    print("=" * 60)
    print()
    
    for json_file in json_files:
        if json_file.exists():
            # 转换为 Excel
            json_to_excel(json_file)
            # 转换为 CSV
            json_to_csv(json_file)
        else:
            print(f"警告: 文件不存在 {json_file.name}\n")
    
    print("=" * 60)
    print("转换完成！")
    print("=" * 60)


if __name__ == "__main__":
    main()

