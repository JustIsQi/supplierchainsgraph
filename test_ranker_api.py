"""
API 测试脚本
使用示例: python test_api.py
"""

import requests
import json
import time

def test_single_rerank(base_url: str = "http://localhost:8000"):
    """测试单条重排序接口"""
    print("\n" + "=" * 50)
    print("测试单条重排序接口")
    print("=" * 50)
    
    request_data = {
        "question": "What goes on the bottom of Shepherd's pie?",
        "context": "Shepherd's pie. History. In early cookery books, the dish was a means of using leftover roasted meat of any kind, and the pie dish was lined on the sides and bottom with mashed potato, as well as having a mashed potato crust on top. Variations and similar dishes. Other potato-topped pies include: The modern 'Cumberland pie' is a version with either beef or lamb and a layer of bread- crumbs and cheese on top."
    }
    
    try:
        print(f"\n请求数据:")
        print(f"问题: {request_data['question']}")
        print(f"上下文长度: {len(request_data['context'])} 字符")
        
        start_time = time.time()
        response = requests.post(
            f"{base_url}/rerank",
            json=request_data,
            headers={"Content-Type": "application/json"}
        )
        elapsed_time = time.time() - start_time
        
        response.raise_for_status()
        
        data = response.json()
        print(f"\n状态码: {response.status_code}")
        print(f"响应时间: {elapsed_time:.3f} 秒")
        print(f"\n响应内容:")
        print(json.dumps(data, indent=2, ensure_ascii=False))
        print("✅ 单条重排序测试成功")
        
        return True
    except Exception as e:
        print(f"❌ 单条重排序失败: {e}")
        return False


def test_batch_rerank(base_url: str = "http://localhost:8000"):
    """测试批量重排序接口"""
    print("\n" + "=" * 50)
    print("测试批量重排序接口")
    print("=" * 50)
    
    request_data = [
        {
            "question": "What goes on the bottom of Shepherd's pie?",
            "context": "The pie dish was lined on the sides and bottom with mashed potato, as well as having a mashed potato crust on top."
        },
        {
            "question": "What is Cumberland pie?",
            "context": "The modern 'Cumberland pie' is a version with either beef or lamb and a layer of breadcrumbs and cheese on top."
        },
        {
            "context": "Shepherd’s pie. History. In early cookery books, the dish was a means of using leftover roasted meat of any kind, and the pie dish was lined on the sides and bottom with mashed potato, as well as having a mashed potato crust on top. Variations and similar dishes. Other potato-topped pies include: The modern ”Cumberland pie” is a version with either beef or lamb and a layer of bread- crumbs and cheese on top. In medieval times, and modern-day Cumbria, the pastry crust had a filling of meat with fruits and spices.. In Quebec, a varia- tion on the cottage pie is called ”Paˆte ́ chinois”. It is made with ground beef on the bottom layer, canned corn in the middle, and mashed potato on top.. The ”shepherdess pie” is a vegetarian version made without meat, or a vegan version made without meat and dairy.. In the Netherlands, a very similar dish called ”philosopher’s stew” () often adds ingredients like beans, apples, prunes, or apple sauce.. In Brazil, a dish called in refers to the fact that a manioc puree hides a layer of sun-dried meat.",
            "question": "What goes on the bottom of Shepherd’s pie?"
        }
    ]
    
    try:
        print(f"\n批量请求数量: {len(request_data)}")
        
        start_time = time.time()
        response = requests.post(
            # f"{base_url}/rerank/batch",
            "http://10.100.0.1:7004/rerank/batch",
            json=request_data,
            headers={"Content-Type": "application/json"}
        )
        elapsed_time = time.time() - start_time
        
        response.raise_for_status()
        
        data = response.json()
        print(f"\n状态码: {response.status_code}")
        print(f"响应时间: {elapsed_time:.3f} 秒")
        print(f"平均每条: {elapsed_time / len(request_data):.3f} 秒")
        print(f"\n响应内容 (前2条):")
        print(type(data),data[0])
        print(json.dumps(data, indent=2, ensure_ascii=False))
        print("✅ 批量重排序测试成功")
        
        return True
    except Exception as e:
        print(f"❌ 批量重排序失败: {e}")
        return False




if __name__ == "__main__":
    import sys
    
    # 从命令行参数获取base_url，默认为localhost:8000
    base_url = "http://10.100.0.1:7004"
    
    # test_single_rerank(base_url)
    test_batch_rerank(base_url)
   
