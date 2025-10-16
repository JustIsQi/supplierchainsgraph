from openai import OpenAI
import json
from pydantic import BaseModel, Field
from typing import List, Optional, Union
from datetime import datetime
import os
import sys,requests
from pathlib import Path

add_path = str(Path(__file__).parent.parent)
sys.path.append(add_path)
os.chdir(add_path)
from models.prompt import WIND_ANNO_PROMPT


# 基础实体模型
class Company(BaseModel):
    """公司实体模型
    对应Tag: Company    加上市公司的注册资本和总资产！！！！！！
    """
    company_name: str = Field(description="公司全称")
    company_abbr: Optional[str] = Field(None, description="公司简称")
    company_name_en: Optional[str] = Field(None, description="公司英文名称")
    company_type: Optional[str] = Field(None, description="企业类型")
    registration_place: Optional[str] = Field(None, description="注册地")
    business_place: Optional[str] = Field(None, description="经营地")
    industry: Optional[str] = Field(None, description="所属行业")
    business_scope: Optional[str] = Field(None, description="经营范围")
    company_qualification: Optional[str] = Field(None, description="公司资质")
    is_bond_issuer: Optional[bool] = Field(None, description="是否债券发行人")
    total_assets: Optional[str] = Field(None, description="总资产")
    registered_capital: Optional[str] = Field(None, description="注册资本")
    
# 股票信息模型
class Stock(BaseModel):
    """股票信息模型
    对应Tag: Stock
    对应Edge:SHAREHOLDING
    """
    stock_code: str = Field(description="股票代码")
    stock_name: str = Field(description="证券名称/股票简称")
    list_status: Optional[str] = Field(None, description="上市公司状态（保持原文表述：正常上市、终止上市、暂缓上市等）")
    list_dt: Optional[str] = Field(None, description="上市日期")
    list_edt: Optional[str] = Field(None, description="退市日期")
    total_share_capital: Optional[str] = Field(None, description="总股本数量（保持原文格式和单位）")
    circulating_share_capital: Optional[str] = Field(None, description="流通股本数量（保持原文格式和单位）")
    stock_type: Optional[str] = Field(None, description="股票种类(上交所、深交所、北交所、港交所、美交所等)")
    exchange: Optional[str] = Field(None, description="股票上市交易所")
    risk_warning_time: Optional[str] = Field(None, description="风险警示时间")
    cancel_risk_warning_time: Optional[str] = Field(None, description="取消风险警示时间")
    risk_warning_status: Optional[str] = Field(None, description="风险警示状态（ST、*ST、-B、RST、QB、QX、退市整理等）")

# 个人信息模型
class Person(BaseModel):
    """人员信息模型（董事、监事、高管等）
    对应Tag: Person
    """
    person_name: str = Field(description="人员名称")
    person_name_en: Optional[str] = Field(None, description="英文人员名称")
    position: Optional[str] = Field(None, description="职位（董事、监事、高管等）")
    birth: Optional[str] = Field(None, description="出生日期")
    education_level: Optional[str] = Field(None, description="学历")
    sex: Optional[str] = Field(None, description="性别")
    compensation: Optional[str] = Field(None, description="个人薪酬（保持原文格式和单位）")
    is_active: Optional[bool] = Field(None, description="是否在职")
    status_change_time: Optional[str] = Field(None, description="状态信息变更时间")

# 股东信息模型
class Shareholder(BaseModel):
    """
    股东信息模型（限售、非限售前十名股东）
    对应Edge: CONTROL_STAKE
    """
    name: str = Field(description="股东名称（自然人姓名或机构名称）")
    shareholder_type: Optional[str] = Field(None, description="股东类型（仅可为：自然人、投资基金、机构投资者）")
    shareholding_percentage: Optional[str] = Field(None, description="持股比例（保持原文格式，如'10.5%'或'10.5'）")
    # shareholding_value: Optional[str] = Field(None, description="持股金额（保持原文格式和单位）")
    currency: Optional[str] = Field(None, description="货币单位 （人民币、美元、港币等）")
    is_major_shareholder: Optional[bool] = Field(None, description="是否为主要股东")
    report_period_change_amount: Optional[str] = Field(None, description="报告期内增减变动数量（保持原文格式和单位）")
    period_end_holdings: Optional[str] = Field(None, description="期末持股数量（保持原文格式和单位）")
    share_type: Optional[str] = Field(None, description="股本类型（限售、非限售）")
    share_percentage: Optional[str] = Field(None, description="股份比例（保持原文格式，如'15.2%'或'15.2'）")
    vote_percentage: Optional[str] = Field(None, description="投票权比例（保持原文格式，如'15.2%'或'15.2'）")
# 子公司信息模型 - 第50行  
class Subsidiary(BaseModel):
    """子公司信息模型  
    """
    subsidiary_name: str = Field(description="子公司名称")
    is_wholly_owned: Optional[bool] = Field(None, description="是否全资子公司")
    subsidiary_type: Optional[str] = Field(None, description="子公司类型（全资子公司、全资孙公司、全资曾孙公司、控股子公司、参股公司等）")
    # subsidiary_relationship: Optional[str] = Field(None, description="子公司关系描述")
    ownership_percentage: Optional[str] = Field(None, description="持股比例（保持原文格式，如'51%'或'51.0'）")
    registration_place: Optional[str] = Field(None, description="注册地")
    business_scope: Optional[str] = Field(None, description="经营范围")
    is_consolidated: Optional[bool] = Field(None, description="是否纳入报表合并范围（true/false）")
    investment_amount: Optional[str] = Field(None, description="投资额（保持原文格式和单位）")
    total_assets: Optional[str] = Field(None, description="总资产（保持原文格式和单位）")
    registered_capital: Optional[str] = Field(None, description="注册资本（保持原文格式和单位）如23,959.28万元")
    investment_method: Optional[str] = Field(None, description="资本投资方式（股权转让、收购兼并、资产剥离、资产交易、资产置换、持有证券、设立）")
    vote_percentage: Optional[str] = Field(None, description="投票权比例（保持原文格式，如'15.2%'或'15.2'）")

# 关联公司信息模型 - 第59行
class RelatedParty(BaseModel):
    """
    关联公司信息模型
    对应Edge: JOINT_VENTURE_OF   关联方可能是人也可以是公司！！！！！！！！！
    """
    related_party_name: str = Field(description="关联方名称")  
    related_party_type: Optional[str] = Field(None, description="关联方类型（合营企业、联营企业、自然人）")

    relationship: Optional[str] = Field(None, description="关联关系描述")
    relationship_percentage: Optional[str] = Field(None, description="关联比例（保持原文格式，如'30%'或'30.0'）")
    business_scope: Optional[str] = Field(None, description="经营范围")

# 供应商信息模型 - 第67行
class Supplier(BaseModel):
    """
    供应商信息模型
    对应Edge: SUPPLIES_TO
    """
    supplier_name: str = Field(description="供应商名称")
    supply_percentage: Optional[str] = Field(None, description="供应商占比（保持原文格式，如'15.3%'或'15.3'）")
    supply_amount: Optional[str] = Field(None, description="供应金额（保持原文格式和单位，如'1,000万元'）")
    currency: Optional[str] = Field(None, description="货币单位")
    supply_content: Optional[str] = Field(None, description="供应内容/产品")
    is_major_supplier: Optional[bool] = Field(None, description="是否为主要供应商")
    report_period: Optional[str] = Field(None, description="报告数据截止日期")

# 客户信息模型 - 第77行
class Customer(BaseModel):
    """
    客户信息模型
    对应Edge: CUSTOMER_OF
    """
    customer_name: str = Field(description="客户名称")
    customer_percentage: Optional[str] = Field(None, description="客户占比（保持原文格式，如'20.5%'或'20.5'）")
    customer_amount: Optional[str] = Field(None, description="客户金额（保持原文格式和单位，如'5,000万元'）")
    currency: Optional[str] = Field(None, description="货币单位")
    business_content: Optional[str] = Field(None, description="业务内容/产品")
    is_major_customer: Optional[bool] = Field(None, description="是否为主要客户")
    report_period: Optional[str] = Field(None, description="报告数据截止日期")

# 主营构成信息模型 - 第87行
class MainBusinessComposition(BaseModel):
    """主营构成信息模型
    对应Tag: Product
    对应Edge：PRODUCES
    """
    product_name: str = Field(description="主营产品名称")
    business_type: Optional[str] = Field(None, description="业务类型（产品分类、行业分类、地区分类）")
    business_country: Optional[str] = Field(None, description="业务所在国家")
    revenue: Optional[str] = Field(None, description="营业收入（保持原文格式和单位，如'2.5亿元'）")
    revenue_percentage: Optional[str] = Field(None, description="收入占比（保持原文格式）")
    gross_profit_margin: Optional[str] = Field(None, description="毛利率（保持原文格式，如'25.8%'或'25.8'）")
    cost: Optional[str] = Field(None, description="营业成本（保持原文格式和单位，如'1.8亿元'）")
    gross_profit: Optional[str] = Field(None, description="毛利（保持原文格式和单位，如'0.7亿元'）")
    currency: Optional[str] = Field(None, description="货币单位")
    report_last_date: Optional[str] = Field(None, description="报告数据截止日期")
    business_description: Optional[str] = Field(None, description="业务描述")

# 综合提取结果模型
class CompanyExtractionResult(BaseModel):
    """公司信息提取结果模型"""
    # 基础公司信息
    company_info: Optional[Company] = Field(None, description="公司基本信息")
    
    # 股票信息
    stock_info: Optional[Stock] = Field(None, description="股票信息")
    
    # 人员信息
    persons: Optional[List[Person]] = Field(None, description="人员信息列表")
    
    # 股东信息
    shareholders: Optional[List[Shareholder]] = Field(None, description="主要股东信息列表")
    
    # 子公司和关联公司信息
    subsidiaries: Optional[List[Subsidiary]] = Field(None, description="子公司信息列表")
    related_companies: Optional[List[RelatedParty]] = Field(None, description="关联公司信息列表")
    
    # 供应商和客户信息
    major_suppliers: Optional[List[Supplier]] = Field(None, description="主要供应商信息列表")
    major_customers: Optional[List[Customer]] = Field(None, description="主要客户信息列表")
    
    # 主营构成信息
    main_business_composition: Optional[List[MainBusinessComposition]] = Field(None, description="主营构成信息列表")
    
    # 报告信息
    report_last_date: Optional[str] = Field(None, description="报告截止日期（格式：年+当前报告期结束时间，如：2023年12月31日）")
    document_type: Optional[str] = Field(None, description="文档类型（如：年报、半年报、一季报、三季报等）")
    
    # 元数据
    # extraction_confidence: Optional[str] = Field(None, description="提取置信度（高/中/低）")
    # data_quality_notes: Optional[List[str]] = Field(None, description="数据质量说明")
    # missing_information: Optional[List[str]] = Field(None, description="缺失信息列表")

def get_models():
    client = OpenAI(
        api_key="Bearer sk-9AiXl4JTI3FCPUIAkEh0Yw", # 在这里将 MOONSHOT_API_KEY 替换为你从 Kimi 开放平台申请的 API Key
        base_url="http://10.102.0.61/v1",
    )
    models = client.models.list()
    return models

def gpt5_infer(message):
    url = "http://10.102.0.61/v1/chat/completions"

    payload = {
        "model": "gpt-5",
        "stream": False,
        "messages": [
            {
                "role": "user",
                "content": message
            }
        ]
    }
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "insomnia/8.4.4",
        "Authorization": "Bearer sk-z6gZt-213cdrE1eCJnW6og"
    }

    response = requests.request("POST", url, json=payload, headers=headers,timeout=10000)

    return response.text

def gpt_chat(message):  
    client = OpenAI(
        api_key="Bearer sk-b6gufWv2tbBiQSWvgcPLmg",
        base_url="http://10.102.0.61/v1",
    )
    try:
        result = client.chat.completions.parse(
            model="gpt-5",
            messages=[
                {"role":"user","content":message}
            ],
            temperature=1.0,
            response_format=CompanyExtractionResult,
            max_completion_tokens=32768,
            timeout=600
        )

        return result.choices[0].message.content
    except Exception as e:
        print(f"API调用错误: {e}")
        return None

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
        response_format=CompanyExtractionResult,
        timeout=3600
    )
    
    return completion.choices[0].message.content

# print(get_models())
# print(qwen_chat("讲个笑话"))

