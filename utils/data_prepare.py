import os,re
import glob
import json
import mistune
from mistune.renderers.markdown import MarkdownRenderer
from mistune import BaseRenderer
from mistune.renderers.rst import RSTRenderer
from langchain_text_splitters import MarkdownHeaderTextSplitter
from bs4 import BeautifulSoup
from models.model_infer import qwen_chat,gpt_chat

def read_all_md_files(directory):
    # 获取所有md文件路径
    md_files = glob.glob(os.path.join(directory, "*.md"))
    
    # 存储文件内容（使用文件名作为key）
    contents = {}
    
    # 逐个读取文件
    for file_path in md_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                # 获取文件名作为key
                file_name = os.path.basename(file_path)
                contents[file_name] = file_path
        except Exception as e:
            print(f"Error reading {file_path}: {str(e)}")
    
    return contents

def read_single_md_file(file_path):
    with open(file_path ,'r', encoding='utf-8') as f:
        md_content = f.read()
    f.close()
    
    img_pattern = r'!\[.*?\]\((.*?)\)'
    filtered_content = re.sub(img_pattern, '', md_content)
    return filtered_content

def read_file(file_path):
    with open(file_path,'r', encoding='utf-8') as f:
        contents = f.readlines()  # 读取所有行，返回一个列表，每个元素是一行的内容，包括换行符\n
    f.close()
    return [content.replace('\n',"").replace('\r',"") for content in contents if content.strip()]

def create_markdown_formatter():
    """创建markdown格式化器"""
    return mistune.create_markdown(renderer=MarkdownRenderer())

def table_to_text(html_content):
    """将HTML表格转换为紧凑的文本格式，并移除多余的空格和换行"""
    soup = BeautifulSoup(html_content, 'html.parser')
    
    for table in soup.find_all('table'):
        text_table_rows = []
        for tr in table.find_all('tr'):
            row_cells = [
                ' '.join(cell.get_text(strip=True).split())
                for cell in tr.find_all(['td', 'th'])
            ]
            if row_cells:
                text_table_rows.append("|".join(row_cells))

        if text_table_rows:
            table.replace_with("\n".join(text_table_rows))

    return soup.decode_contents()


def convert_html_table_to_json(html_content):
    json_format = json.dumps(
        {
            "table":[
                {
                    "姓名":"张三",
                    "年龄":"20",
                    "性别":"男"
                },
                {
                    "姓名":"李四",
                    "年龄":"21",
                    "性别":"女"
                }
            ]

        },
        indent=4,
        ensure_ascii=False
    )
    prompt = f"""输入以下文本，文本中可能包含HTML Table，请识别出表格，将HTML Table转换为JSON格式文本，并用JSON文本替换HTML Table，输出替换后的文本,注意：非表格部分的内容也要输出。
    HTML Table转换成的JSON格式为：{json_format}
    {html_content}
    """

    response = qwen_chat(prompt)
    return response

def Markdown2Text_with_header(md_content):
    """Splitter 切割text"""
    img_pattern = r'!\[.*?\]\(.*?\)'
    filtered_content = re.sub(img_pattern, '', md_content)

    # 定义需要识别的Markdown标题级别
    headers_to_split_on = [
        ("#", "Header 1"),    # 一级标题
        ("##", "Header 2"),   # 二级标题 
        ("###", "Header 3"),  # 三级标题
    ]

    # 初始化结果列表
    # 创建Markdown分割器，保留标题信息
    markdown_splitter = MarkdownHeaderTextSplitter(
        headers_to_split_on=headers_to_split_on,
        strip_headers=False,      # 保留标题文本
        return_each_line=True     # 返回每行内容
    )
    # 分割Markdown内容
    md_header_splits = markdown_splitter.split_text(filtered_content)
    headers = [list(split.metadata.values())[0] for split in md_header_splits if len(split.metadata.values()) > 0]
    
    if headers:
        contents = {}
        for split in md_header_splits:
            if len(split.metadata.values()) > 0:
                header = list(split.metadata.values())[0]   
                content = split.page_content.strip()

                # if "<table" in content:
                #     content = convert_html_table_to_json(content).strip()
                # else:
                #     content = content.strip()
                
                if header not in contents:
                    contents[header] = content
                else:
                    contents[header] += content+'\n'
    else:
        contents = [split.page_content.strip() for split in md_header_splits]
            
    return contents

def Markdown2Text(md_content):
    """Splitter 切割text"""
    img_pattern = r'!\[.*?\]\(.*?\)'
    filtered_content = re.sub(img_pattern, '', md_content)

    # 定义需要识别的Markdown标题级别
    headers_to_split_on = [
        ("#", "Header 1"),    # 一级标题
        ("##", "Header 2"),   # 二级标题 
        ("###", "Header 3"),  # 三级标题
    ]

    # 初始化结果列表
    # 创建Markdown分割器，保留标题信息
    markdown_splitter = MarkdownHeaderTextSplitter(
        headers_to_split_on=headers_to_split_on,
        strip_headers=False,      # 保留标题文本
        return_each_line=True     # 返回每行内容
    )
    # 分割Markdown内容
    md_header_splits = markdown_splitter.split_text(filtered_content)

    contents = [split.page_content.strip() for split in md_header_splits]
    return contents

def Markdown_header_splits(md_content):
    """Splitter 切割text，提取并返回三级标题"""
    img_pattern = r'!\[.*?\]\(.*?\)'
    filtered_content = re.sub(img_pattern, '', md_content)

    # 定义需要识别的Markdown标题级别（只到三级标题）
    headers_to_split_on = [
        # ("#", "Header 1"),    # 一级标题
        ("##", "Header 2"),   # 二级标题 
        ("###", "Header 3"),  # 三级标题
    ]

    # 创建Markdown分割器，保留标题信息
    markdown_splitter = MarkdownHeaderTextSplitter(
        headers_to_split_on=headers_to_split_on,
        strip_headers=False,      # 不移除标题文本，便于提取
        return_each_line=True     # 返回每行内容
    )
    
    # 分割Markdown内容
    md_header_splits = markdown_splitter.split_text(filtered_content)

    # 提取所有三级及以上的标题
    headers,contents = [],[]
    for split in md_header_splits:
        if len(split.metadata.values()) > 0:
            header_text = list(split.metadata.values())[0]
            print(header_text)
            content = split.page_content.strip()
            headers.append(header_text)
            contents.append(content)
    
    return headers,contents

def content_to_kv(markdown_text):
    markdown_formatter = create_markdown_formatter()
    formatted_result = markdown_formatter(markdown_text.strip())
    contents = Markdown2Text(formatted_result)
    texts = [
        table_to_text(content).strip() if "<table" in content else content.strip()
        for content in contents
    ]
    return texts

def content_to_kv_json(markdown_text):
    """使用JSON格式处理表格的版本"""
    markdown_formatter = create_markdown_formatter()
    formatted_result = markdown_formatter(markdown_text.strip())
    contents = Markdown2Text(formatted_result)
    texts = [
        convert_html_table_to_json(content) if "<table" in content else content
        for content in contents
    ]
    return texts

# 测试函数示例
# def test_table_conversion():
#     """测试HTML表格转换功能"""
#     # 示例HTML内容，包含表格和文本
#     html_sample = '''
#   一、合营企业</th></tr></thead><tbody><tr><td>北京盛德大业新能源动力科技有限公司</td><td class="right">18,141,642.61</td><td class="right"></td><td></td><td class="right">-3,335,028.21</td><td></td><td></td><td></td><td></td><td class="right"></td><td class="right">14,806,614.40</td><td></td></tr><tr><td>小计</td><td class="right">18,141,642.61</td><td class="right"></td><td></td><td class="right">-3,335,028.21</td><td></td><td></td><td></td><td></td><td class="right"></td><td class="right">14,806,614.40</td><td></td></tr><tr><td colspan="12">二、联营企业</td></tr><tr><td>北京普莱德新材料有限公司</td><td class="right">72,622,187.35</td><td class="right"></td><td></td><td class="right">6,034.18</td><td></td><td></td><td></td><td></td><td class="right"></td><td class="right">72,628,221.53</td><td></td></tr><tr><td>Valmet</td><td class="right">179,956,957.31</td><td class="right"></td><td></td><td class="right">24,637,260.20</td><td></td><td></td><td></td><td></td><td class="right"></td><td class="right">204,594,217.51</td><td></td></tr><tr><td>上汽时代动力电池系统有限公司</td><td class="right">145,928,349.68</td><td class="right"></td><td></td><td class="right">-1,007,308.17</td><td></td><td></td><td></td><td></td><td class="right"></td><td class="right">144,921,041.51</td><td></td></tr><tr><td>宁波梅山保税港区晨道投资合伙企业（有限合伙）</td><td class="right">1,122,000.00</td><td class="right"></td><td></td><td class="right">3,597,925.10</td><td></td><td></td><td></td><td></td><td class="right"></td><td class="right">4,719,925.10</td><td></td></tr><tr><td>NAL</td><td class="right">373,256,083.95</td><td class="right"></td><td></td><td class="right">-4,765,875.39</td><td></td><td></td><td></td><td></td><td class="right">20,240.76</td><td class="right">368,510,449.32</td><td></td></tr><tr><td>NAN（注1）</td><td class="right"></td><td class="right">73,842,000.00</td><td></td><td class="right">-269,863.77</td><td></td><td></td><td></td><td></td><td class="right"></td><td class="right">73,572,136.23</td><td></td></tr><tr><td>北京新能源汽车技术创新中心有限公司(注2)</td><td class="right"></td><td class="right">30,000,000.00</td><td></td><td class="right"></td><td></td><td></td><td></td><td></td><td class="right"></td><td class="right">30,000,000.00</td><td></td></tr><tr><td>小计</td><td class="right">772,885,578.29</td><td class="right">103,842,000.00</td><td></td><td class="right">22,198,172.15</td><td></td><td></td><td></td><td></td><td class="right">20,240.76</td><td class="right">898,945,991.20</td><td></td></tr><tr><td>合计</td><td class="right">791,027,220.90</td><td class="right">103,842,000.00</td><td></td><td class="right">18,863,143.94</td><td></td><td></td><td></td><td></td><td class="right">20,240.76</td><td class="right">913,752,605.60</td><td></td></tr></tbody></table>
#     '''
    
#     print("原始文本格式：")
#     # print(table_to_text(html_sample))
#     # print("\n" + "="*50 + "\n")
#     # print("结构化JSON格式：")
#     # print(table_to_json(html_sample))
#     # print("\n" + "="*50 + "\n")
#     print("内联JSON格式（表格直接替换为JSON）：")  
#     print(convert_html_table_to_json(html_sample))
    
# test_table_conversion()  
