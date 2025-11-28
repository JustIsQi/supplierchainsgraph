"""
配置文件 - 海外上市公司财报数据抽取
"""

# ============================================================================
# Rerank配置
# ============================================================================

RERANK_CONFIG = {
    # Rerank API配置
    "api_url": "http://10.100.0.205:4000/rerank",
    "api_key": "sk-1234",
    "model": "Bge-ReRanker",
    "timeout": 60,
    
    # 段落分割配置
    "max_paragraph_length": 3000,  # 每个段落最大字符数
    
    # Rerank筛选配置
    "enable_rerank": True,  # 是否启用rerank
    "top_k": 15,  # 选择最相关的段落数量
    "score_threshold": 0.3,  # 相关性分数阈值（0.0-1.0）
    
    # Rerank查询文本
    "query": "Extract production operations revenue financial data business segments from financial report",
    
    # 当段落数量少于此值时，跳过rerank
    "min_paragraphs_for_rerank": 10
}

# ============================================================================
# LLM模型配置
# ============================================================================

QWEN_CONFIG = {
    "api_key": "sk-1234",
    "base_url": "http://10.100.0.205:4000",
    "model": "Qwen3-30B-A3B-Thinking-2507",
    "temperature": 0.6,
    "top_p": 0.95,
    "extra_body": {"top_k": 20, "min_p": 0.0},
    "timeout": 3600
}

GPT_OSS_CONFIG = {
    "api_key": "EMPTY",
    "base_url": "http://10.100.0.2:8002/v1",
    "model": "gptoss",
    "temperature": 0.6,
    "top_p": 0.95,
    "timeout": 3600
}

# ============================================================================
# 路径配置
# ============================================================================

PATH_CONFIG = {
    # 输入文件目录
    "datasets_dir": "/data/share2/yy/workspace/code/supplierchainsgraph/oversea_study/datasets",
    
    # 输出结果目录
    "results_dir": "/data/share2/yy/workspace/code/supplierchainsgraph/oversea_study/results",
    
    # 本地测试路径（如果需要在本地运行）
    "local_datasets_dir": "oversea_study/datasets",
    "local_results_dir": "oversea_study/results"
}

# ============================================================================
# 处理配置
# ============================================================================

PROCESSING_CONFIG = {
    # 并行处理的模型数量
    "max_workers": 2,
    
    # 是否启用多模型推理
    "enable_multi_model": True,
    
    # 启用的模型列表
    "enabled_models": ["qwen", "gpt-oss"],  # 可选: "qwen", "gpt-oss"
    
    # 是否保存中间结果（rerank后的文档）
    "save_filtered_content": False,
    
    # 日志级别
    "log_level": "INFO"  # DEBUG, INFO, WARNING, ERROR
}

# ============================================================================
# 调试配置
# ============================================================================

DEBUG_CONFIG = {
    # 是否启用调试模式
    "debug_mode": False,
    
    # 调试模式下只处理前N个文件
    "debug_max_files": 1,
    
    # 是否打印完整的prompt
    "print_full_prompt": False,
    
    # 是否打印rerank详细信息
    "print_rerank_details": True
}

# ============================================================================
# 辅助函数
# ============================================================================

def get_rerank_config():
    """获取rerank配置"""
    return RERANK_CONFIG.copy()

def get_qwen_config():
    """获取Qwen模型配置"""
    return QWEN_CONFIG.copy()

def get_gpt_oss_config():
    """获取GPT-OSS模型配置"""
    return GPT_OSS_CONFIG.copy()

def get_path_config(use_local=False):
    """
    获取路径配置
    
    Args:
        use_local: 是否使用本地路径
    """
    if use_local:
        return {
            "datasets_dir": PATH_CONFIG["local_datasets_dir"],
            "results_dir": PATH_CONFIG["local_results_dir"]
        }
    return {
        "datasets_dir": PATH_CONFIG["datasets_dir"],
        "results_dir": PATH_CONFIG["results_dir"]
    }

def get_processing_config():
    """获取处理配置"""
    return PROCESSING_CONFIG.copy()

def get_debug_config():
    """获取调试配置"""
    return DEBUG_CONFIG.copy()

# ============================================================================
# 配置验证
# ============================================================================

def validate_config():
    """验证配置是否合理"""
    errors = []
    warnings = []
    
    # 验证rerank配置
    if RERANK_CONFIG["score_threshold"] < 0 or RERANK_CONFIG["score_threshold"] > 1:
        errors.append("score_threshold 必须在 0-1 之间")
    
    if RERANK_CONFIG["top_k"] < 1:
        errors.append("top_k 必须大于 0")
    
    if RERANK_CONFIG["max_paragraph_length"] < 500:
        warnings.append("max_paragraph_length 较小可能导致段落过于碎片化")
    
    # 验证模型配置
    if QWEN_CONFIG["temperature"] < 0 or QWEN_CONFIG["temperature"] > 2:
        warnings.append("temperature 建议在 0-1 之间")
    
    # 验证处理配置
    if PROCESSING_CONFIG["max_workers"] < 1:
        errors.append("max_workers 必须大于 0")
    
    if not PROCESSING_CONFIG["enabled_models"]:
        errors.append("至少需要启用一个模型")
    
    return errors, warnings

if __name__ == "__main__":
    # 测试配置
    errors, warnings = validate_config()
    
    if errors:
        print("❌ 配置错误:")
        for error in errors:
            print(f"  - {error}")
    
    if warnings:
        print("⚠️  配置警告:")
        for warning in warnings:
            print(f"  - {warning}")
    
    if not errors and not warnings:
        print("✅ 配置验证通过")
    
    # 打印配置摘要
    print("\n配置摘要:")
    print(f"  Rerank: {'启用' if RERANK_CONFIG['enable_rerank'] else '禁用'}")
    print(f"  Top-K: {RERANK_CONFIG['top_k']}")
    print(f"  分数阈值: {RERANK_CONFIG['score_threshold']}")
    print(f"  段落长度: {RERANK_CONFIG['max_paragraph_length']}")
    print(f"  启用模型: {', '.join(PROCESSING_CONFIG['enabled_models'])}")

