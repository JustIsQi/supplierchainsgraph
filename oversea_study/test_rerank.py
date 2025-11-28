"""
测试Rerank功能
用于验证段落分割和rerank筛选是否正常工作
"""

import sys
from pathlib import Path
import json

# 添加项目路径
add_path = str(Path(__file__).parent.parent)
sys.path.append(add_path)

# 从test.py导入函数
from oversea_study.test import split_text_into_paragraphs, rerank_paragraphs, preprocess_document
from utils.data_prepare import read_single_md_file

def test_split_paragraphs():
    """测试段落分割功能"""
    print("=" * 80)
    print("测试1: 段落分割功能")
    print("=" * 80)
    
    # 测试文本
    test_text = """
# Title 1
This is the first paragraph. It contains some text about business operations.

This is the second paragraph. It talks about revenue and financial data.

# Title 2
This is the third paragraph with more detailed information about the company's 
financial performance, including revenue growth, operating expenses, and net income.
The paragraph is long enough to test the splitting logic.

This is a short paragraph.

# Title 3
Another section with tables and data:
Revenue: $1,000,000
Expenses: $500,000
Net Income: $500,000
    """.strip()
    
    print(f"\n原始文本长度: {len(test_text)} 字符\n")
    
    # 分割段落
    paragraphs = split_text_into_paragraphs(test_text, max_length=200)
    
    print(f"分割结果: {len(paragraphs)} 个段落\n")
    for i, para in enumerate(paragraphs, 1):
        print(f"段落 {i} ({len(para)} 字符):")
        print(f"  {para[:100]}..." if len(para) > 100 else f"  {para}")
        print()
    
    return True

def test_rerank_function():
    """测试rerank筛选功能"""
    print("=" * 80)
    print("测试2: Rerank筛选功能")
    print("=" * 80)
    
    # 测试段落列表
    paragraphs = [
        "The company reported strong revenue growth in Q4 2023, with net sales reaching $574.8 billion.",
        "Our headquarters is located in Seattle, Washington. We have offices worldwide.",
        "Operating income increased by 201% year-over-year to $36.9 billion in fiscal 2023.",
        "The company values diversity and inclusion in the workplace.",
        "North America segment generated $315.9 billion in net sales, up 12% from prior year.",
        "We are committed to reducing our carbon footprint and achieving net-zero emissions.",
        "AWS segment operating income was $24.6 billion, representing 67% of total operating income.",
        "Employee benefits include health insurance, retirement plans, and stock options.",
        "Gross profit margin improved from 43.8% to 47.6% year-over-year.",
        "The company supports various community programs and charitable initiatives."
    ]
    
    print(f"\n输入段落数: {len(paragraphs)}")
    print("\n段落内容:")
    for i, para in enumerate(paragraphs, 1):
        print(f"  {i}. {para[:60]}...")
    
    # 执行rerank
    query = "Extract production operations revenue financial data business segments from financial report"
    print(f"\n查询文本: {query}")
    
    selected = rerank_paragraphs(
        paragraphs,
        query=query,
        top_k=5,
        score_threshold=0.3
    )
    
    print(f"\n筛选后段落数: {len(selected)}")
    print("\n筛选后的段落:")
    for i, para in enumerate(selected, 1):
        print(f"  {i}. {para}")
        print()
    
    return True

def test_full_document():
    """测试完整文档处理"""
    print("=" * 80)
    print("测试3: 完整文档处理")
    print("=" * 80)
    
    # 尝试读取实际的文档
    results_dir = Path("/data/share2/yy/workspace/code/supplierchainsgraph/oversea_study/results")
    local_results_dir = Path("oversea_study/results")
    
    # 尝试找到测试文件
    test_file = None
    for base_dir in [results_dir, local_results_dir]:
        if base_dir.exists():
            md_files = list(base_dir.glob("*.md"))
            if md_files:
                test_file = md_files[0]
                break
    
    if not test_file:
        print("⚠️  未找到测试文件，跳过此测试")
        return False
    
    print(f"\n测试文件: {test_file.name}")
    
    try:
        # 读取文件
        md_content = read_single_md_file(str(test_file))
        print(f"原始文档: {len(md_content)} 字符")
        
        # 预处理文档
        filtered_content = preprocess_document(md_content, enable_rerank=True)
        
        print(f"处理后文档: {len(filtered_content)} 字符")
        print(f"压缩率: {(1 - len(filtered_content) / len(md_content)) * 100:.1f}%")
        
        # 显示前500字符
        print("\n处理后内容预览:")
        print("-" * 80)
        print(filtered_content[:500])
        print("...")
        print("-" * 80)
        
        return True
        
    except Exception as e:
        print(f"❌ 测试失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def test_rerank_api():
    """测试Rerank API连接"""
    print("=" * 80)
    print("测试4: Rerank API连接")
    print("=" * 80)
    
    import requests
    
    try:
        # 简单的测试请求
        data = {
            "model": "Bge-ReRanker",
            'query': "financial report",
            'documents': [
                "Revenue was $100 million",
                "The weather is sunny today",
                "Operating expenses decreased by 5%"
            ]
        }
        
        headers = {
            "accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer sk-1234",
        }
        
        print("\n发送测试请求到 http://10.100.0.205:4000/rerank ...")
        response = requests.post(
            'http://10.100.0.205:4000/rerank',
            headers=headers,
            json=data,
            timeout=10
        )
        
        if response.status_code == 200:
            print("✅ API连接成功")
            results = json.loads(response.text)['results']
            print(f"\n返回结果:")
            for r in results:
                print(f"  - Index: {r['index']}, Score: {r['relevance_score']:.3f}")
            return True
        else:
            print(f"❌ API请求失败 (状态码: {response.status_code})")
            print(f"响应内容: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ API连接失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """运行所有测试"""
    print("\n" + "=" * 80)
    print("Rerank功能测试套件")
    print("=" * 80 + "\n")
    
    tests = [
        ("段落分割", test_split_paragraphs),
        ("Rerank API连接", test_rerank_api),
        ("Rerank筛选", test_rerank_function),
        ("完整文档处理", test_full_document),
    ]
    
    results = []
    for name, test_func in tests:
        try:
            success = test_func()
            results.append((name, success))
        except Exception as e:
            print(f"\n❌ {name} 测试异常: {str(e)}")
            import traceback
            traceback.print_exc()
            results.append((name, False))
        print()
    
    # 打印测试总结
    print("\n" + "=" * 80)
    print("测试总结")
    print("=" * 80)
    
    for name, success in results:
        status = "✅ 通过" if success else "❌ 失败"
        print(f"{status}  {name}")
    
    total = len(results)
    passed = sum(1 for _, success in results if success)
    print(f"\n总计: {passed}/{total} 测试通过")
    
    if passed == total:
        print("\n🎉 所有测试通过！")
    else:
        print(f"\n⚠️  {total - passed} 个测试失败，请检查错误信息")

if __name__ == "__main__":
    main()

