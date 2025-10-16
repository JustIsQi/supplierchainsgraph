WIND_ANNO_PROMPT = """
你是一个专业的金融文档信息提取助手。请仔细分析以下报告内容，提取关键的公司信息。

## 提取要求：
1. **公司基本信息**：
   - company_name: 公司全称（使用完整的法定名称）
   - company_abbr: 公司简称
   - company_name_en: 公司英文名称
   - company_type: 企业类型
   - registration_place: 注册地
   - business_place: 经营地
   - industry: 所属行业
   - business_scope: 经营范围
   - company_qualification: 公司资质
   - is_bond_issuer: 是否债券发行人（true/false）
   - total_assets: 总资产（保持原文格式和单位）
   - registered_capital: 注册资本（原文格式+单位，如23,959.28万元）

2. **股票信息**：
   - stock_code: 股票代码
   - stock_name: 证券名称/股票简称
   - list_status: 上市公司状态（保持原文表述：正常上市、终止上市、暂缓上市、退市整理等）
   - list_dt: 上市日期
   - list_edt: 退市日期
   - total_share_capital: 总股本数量（保持原文格式和单位）
   - circulating_share_capital: 流通股本数量（保持原文格式和单位）
   - stock_type: 股票种类（上交所、深交所、北交所、港交所、美交所等）
   - exchange: 股票上市交易所
   - risk_warning_time: 风险警示时间
   - cancel_risk_warning_time: 取消风险警示时间
   - risk_warning_status: 风险警示状态（ST、*ST、-B、RST、QB、QX、退市整理等）

3. **董事、监事、高管人员信息**：
   - person_name: 人员名称
   - person_name_en: 英文人员名称
   - position: 职位（董事、监事、高管等）
   - birth: 出生日期
   - education_level: 学历
   - sex: 性别
   - compensation: 个人薪酬（保持原文格式和单位）
   - is_active: 是否在职（true为在职，false为离职）
   - status_change_time: 状态信息变更时间

4. **前十名股东信息（限售、非限售）**：
   - name: 股东名称（自然人姓名或机构名称）
   - shareholder_type: 股东类型（仅可为：自然人、投资基金、机构投资者）
   - shareholding_percentage: 持股比例（保持原文格式，如'10.5%'或'10.5'）
   - currency: 货币单位（人民币、美元、港币、元、万元等）
   - is_major_shareholder: 是否为主要股东（true/false）
   - report_period_change_amount: 报告期内增减变动数量（保持原文格式和单位）
   - period_end_holdings: 期末持股数量（保持原文格式和单位）
   - share_type: 股本类型（限售、非限售）
   - share_percentage: 股份比例（保持原文格式，如'15.2%'或'15.2'）
   - vote_percentage: 投票权比例（保持原文格式，如'15.2%'或'15.2'）;特别注意：投票权比例不等于持股比例，只有文中特别写明了投票权比例才需要提取，未提及时请默认为null

5. **子公司信息**：
   - subsidiary_name: 子公司名称
   - is_wholly_owned: 是否全资子公司（true/false）
   - subsidiary_type: 子公司类型（全资子公司、全资孙公司、全资曾孙公司、控股子公司、参股公司等）
   - ownership_percentage: 持股比例（保持原文格式，如'51%'或'51.0'）
   - registration_place: 注册地
   - business_scope: 经营范围
   - is_consolidated: 是否纳入报表合并范围（true/false）
   - investment_amount: 投资额（保持原文格式和单位）
   - total_assets: 总资产（保持原文格式和单位）
   - registered_capital: 注册资本（原文格式+单位，如23,959.28万元）
   - investment_method: 资本投资方式（股权转让、收购兼并、资产剥离、资产交易、资产置换、持有证券、设立）
   - vote_percentage: 投票权比例（保持原文格式，如'15.2%'或'15.2'）；特别注意：投票权比例不等于持股比例，只有文中特别写明了投票权比例才需要提取，未提及时请默认为null


6. **关联方信息**：
   - related_party_name: 关联方名称
   - related_party_type: 关联方类型（请根据关联方的实际性质准确识别：若关联方是具体的公司、企业、机构等法人实体，则根据其控制关系标注为"合营企业"或"联营企业"；若关联方是具体的个人姓名，则标注为"自然人"。判断时请重点关注关联方名称的特征：包含"有限公司"、"股份有限公司"、"集团"、"企业"、"投资"、"控股"等字样的通常为企业法人实体，应标注为合营企业或联营企业；明确的个人姓名则标注为自然人）
   - relationship: 关联关系描述(保留原文表述：如实际控制人近亲属之亲属控制的公司；持股5%以上股东)
   - relationship_percentage: 关联比例（保持原文格式，如'30%'或'30.0'）
   - business_scope: 经营范围

7. **供应商信息**：
   - supplier_name: 供应商名称（严格要求：仅抽取具体明确的公司全称或自然人姓名。不得抽取以下类型的模糊指代：1）序号型指代如"第一供应商"、"第二供应商"、"供应商一"、"供应商二"等；2）字母型指代如"A公司"、"B公司"、"甲方"、"乙方"等；3）其他非具体名称的指代。只有当原文中明确给出具体的公司全称（如"北京科技股份有限公司"）或自然人姓名时才可抽取，否则该字段设为null）
   - supply_percentage: 供应商占比（保持原文格式，如'15.3%'或'15.3'）
   - supply_amount: 供应金额（保持原文格式和单位，如'1,000万元'）
   - currency: 货币单位
   - supply_content: 供应内容/产品
   - is_major_supplier: 是否为主要供应商（true/false）
   - report_period: 报告数据截止日期

8. **客户信息**：
   - customer_name: 客户名称（严格要求：仅抽取具体明确的公司全称或自然人姓名。不得抽取以下类型的模糊指代：1）序号型指代如"客户一"、"客户二"、"第一客户"、"第二客户"等；2）字母型指代如"A公司"、"B公司"、"甲方"、"乙方"等；3）其他非具体名称的指代。只有当原文中明确给出具体的公司全称（如"上海贸易有限公司"）或自然人姓名时才可抽取，否则该字段设为null）
   - customer_percentage: 客户占比（保持原文格式，如'20.5%'或'20.5'）
   - customer_amount: 客户金额（保持原文格式和单位，如'5,000万元'）
   - currency: 货币单位
   - business_content: 业务内容/产品
   - is_major_customer: 是否为主要客户（true/false）
   - report_period: 报告数据截止日期

9. **主营构成信息**（按产品分类、行业分类、地区分类等多维度全面提取）：
   - product_name: 主营产品名称
   - business_type: 业务类型（必须明确标注分类方式：产品分类、行业分类、地区分类等）
   - business_country: 业务所在国家
   - revenue: 营业收入（保持原文格式和单位，如'2.5亿元'）
   - revenue_percentage: 收入占比（保持原文格式）
   - gross_profit_margin: 毛利率（保持原文格式，如'25.8%'或'25.8'）
   - cost: 营业成本（保持原文格式和单位，如'1.8亿元'）
   - gross_profit: 毛利（保持原文格式和单位，如'0.7亿元'）
   - currency: 货币单位
   - report_last_date: 报告数据截止日期
   - business_description: 业务描述
   
   **重要说明**：如果报告中存在多种分类维度的主营构成（如既有按产品分类，又有按行业分类或地区分类），必须将所有分类维度的数据全部提取出来，每种分类维度生成独立的记录条目。例如：
   - 按产品分类：手机业务、电脑业务等
   - 按地区分类：国内业务、海外业务等
   - 按行业分类：制造业、服务业等
   每个分类维度的每个条目都应该作为独立的main_business_composition记录

10. **报告截止日期**：报告发布数据的截止时间，输出格式为年+当前报告期的结束时间（一季度：3月31日；半年报：6月30日；三季报：9月30日；年报：12月31日）

11. **文档类型**：识别文档类型（如：年报、半年报、一季报、三季报等）

## 数据格式要求：
- **所有数值字段必须保持原文格式**：包括数字、百分比、金额等，完全按照原文中的表述提取，不允许任何形式的换算、计算或格式转换
- **比例字段格式要求**：
  - shareholding_percentage（持股比例）、customer_percentage（客户占比）、supply_percentage（供应商占比）、relationship_percentage（关联比例）、ownership_percentage（持股比例）、share_percentage（股份比例）等请保持原文格式（如原文是"10.5%"则提取"10.5%"，如原文是"10.5"则提取"10.5"）
- **金额字段格式要求**：
  - supply_amount（供应金额）、customer_amount（客户金额）、investment_amount（投资额）、total_assets（总资产）、registered_capital（注册资本）、revenue（营业收入）、cost（营业成本）、gross_profit（毛利）、compensation（个人薪酬）等必须保持原文的确切表述和单位（如"1,000万元"、"2.5亿"、"23,959.28万元"等）
- **名称字段要求**：
  - company_name（公司全称）、subsidiary_name（子公司名称）等请使用完整的法定名称或原文准确表述
  - supplier_name（供应商名称）、customer_name（客户名称）必须是具体明确的公司全称或自然人姓名，严禁抽取模糊指代（如"第一供应商"、"客户一"、"A公司"、"B公司"等），若原文无具体名称则设为null
- **分类字段严格要求**：
  - shareholder_type（股东类型）请严格区分遵循原文表述：自然人、投资基金、机构投资者（仅限这三个选项）
  - subsidiary_type（子公司类型）请严格区分遵循原文表述：全资子公司、控股子公司、参股公司等
  - related_party_type（关联方类型）请准确识别关联方的实际性质：若关联方是企业法人实体（包含"有限公司"、"股份有限公司"、"集团"、"企业"、"投资"、"控股"等字样），根据控制关系标注为"合营企业"或"联营企业"；若关联方是明确的个人姓名，则标注为"自然人"（仅限这三个选项）
  - list_status（上市公司状态）请严格区分遵循原文表述：正常上市、终止上市、暂缓上市、退市整理等
  - position（人员职位）请明确标注：董事、监事、高管等具体职位
  - share_type（股本类型）请严格区分遵循原文表述：限售、非限售
  - business_type（业务类型）请明确标注分类方式：产品分类、行业分类、地区分类等，确保不同分类维度的数据都被完整提取
  - investment_method（资本投资方式）请区分：股权转让、收购兼并、资产剥离、资产交易、资产置换、持有证券、设立
  - risk_warning_status（风险警示状态）请明确：ST、*ST、-B、RST、QB、QX、退市整理等
- **布尔字段要求**：
  - is_bond_issuer（是否债券发行人）、is_wholly_owned（是否全资子公司）、is_major_shareholder（是否为主要股东）、is_consolidated（是否纳入报表合并范围）、is_major_supplier（是否为主要供应商）、is_major_customer（是否为主要客户）、is_active（是否在职）等字段，请明确使用true/false值

## 严格的质量控制和输出约束：
**【核心原则】：严禁胡乱生成任何信息，所有输出必须有明确的文档依据**

1. **信息来源验证**：
   - 提取的所有信息必须来自于文档内容，绝对禁止无中生有
   - 每个数据字段都必须在原文中有明确对应的文字或数字
   - 如果文档中没有明确提及某个信息，必须将该字段设为null

2. **数值严格控制**：
   - **绝对禁止任何计算**：所有数值（金额、比例、数量、日期等）必须完全来自原文，严禁任何推算、估算、近似、计算或换算
   - **严禁数值格式转换**：必须保持原文的确切表述，包括小数点位数、千分位分隔符、单位表示等
   - **严禁单位换算**：如果原文中的数值单位不统一，保持原文单位，绝对不进行单位换算（如万元不能换算成元）。如果原文中有单位，则在数值后面加上单位。
   - **严禁比例计算**：比例数据必须按原文表述提取，绝对不得自行计算或推导（如不能用营业收入除以总收入计算占比）
   - **严禁毛利计算**：即使有营业收入和营业成本，也不允许计算毛利，只能提取原文中明确给出的毛利数值
   - **数值示例**：
     - 原文："持股比例为35.67%"→提取："35.67%"
     - 原文："营业收入1,234.56万元"→提取："1,234.56万元"
   
3. **文本严格对应**：
   - 公司名称、人员姓名等必须与原文完全一致，不允许任何形式的改写或简化
   - 地址、经营范围等描述性信息必须原文引用，不得意译或概括
   - 专业术语和行业名称必须保持原文用词

4. **缺失信息处理**：
   - 如果原文中没有提及的信息，坚决设为null，不要试图推测或补充
   - 宁可留空也不要生成可能不准确的信息
   - 对于模糊不清或存在歧义的信息，标记为null

5. **验证机制**：
   - 在提取每个信息后，必须能够在原文中找到对应的确切表述
   - 对于所有提取的数值，必须确保与原文数字和格式完全匹配
   - 禁止基于上下文推断或常识判断来填充缺失信息

**警告：任何形式的信息臆造、数值编造、计算推导或推测性填充都是严格禁止的。所有数值必须原文提取，不允许任何计算操作。当不确定时，选择null而不是猜测。**

## 文档内容：
{contents}  
"""

COMPANY_SEARCH_PROMPT = """
请分析公司"{raw_org}"的详细信息，按照以下逻辑进行全面的企业关系网络分析：

1. 工商注册信息查询：
- 如果"{raw_org}"是公司简称，请查找其工商注册的公司全称

2. 上市情况分析：
- 判断该公司是否为上市公司
- 如果是上市公司，查找其上市交易所和股票代码
- 如果不是上市公司，继续第3步

3. 完整企业关系网络分析：
- 查找该公司的完整股权架构链条，并总结股权架构
- 识别所有层级的母公司（直接控股母公司、间接控股母公司等）
- 识别所有重要的子公司（直接控股子公司、间接控股子公司等）
- 识别重要的关联公司（同一控制人下的兄弟公司、合资公司、参股公司等）
- 对每个相关企业分别判断是否为上市公司
- 如果某个相关企业是上市公司，获取其上市交易所和股票代码
- 提供每个企业的持股比例或关联关系信息

4. 输出要求：
- 必须使用JSON格式输出
- 包含完整的公司信息和企业关系网络信息
- 确保所有信息准确、完整

输出格式：
{{
    "company_info": {{
        "company_name": "工商注册的公司全称",
        "is_listed": true/false,
        "exchange": "上市交易所名称（如适用）",
        "stock_code": "股票代码（如适用）",
        "organizational_structure_summary":"企业关系网络总结",
        "parent_companies": [
            {{
                "company_name": "母公司名称",
                "is_listed": true/false,
                "exchange": "上市交易所（如适用）",
                "stock_code": "股票代码（如适用）",
                "ownership_percentage": "持股比例",
                "relationship_type": "direct_parent/indirect_parent/ultimate_parent"
            }}
        ],
        "subsidiary_companies": [
            {{
                "company_name": "子公司名称",
                "is_listed": true/false,
                "exchange": "上市交易所（如适用）",
                "stock_code": "股票代码（如适用）",
                "ownership_percentage": "持股比例",
                "relationship_type": "direct_subsidiary/indirect_subsidiary"
            }}
        ],
        "affiliated_companies": [
            {{
                "company_name": "关联公司名称",
                "is_listed": true/false,
                "exchange": "上市交易所（如适用）",
                "stock_code": "股票代码（如适用）",
                "ownership_percentage": "持股比例或关联度",
                "relationship_type": "sister_company/joint_venture/associate_company/other",
                "relationship_description": "关联关系描述"
            }}
        ]
    }}
}}

注意：
- 只输出JSON格式，不要添加任何解释文字
- 公司名称要使用官方工商注册全称
- 如果无法确定某项信息，该字段设为null或空字符串
- 如果没有相关企业信息，对应数组为空数组[]
- 每个数组中应包含所有相关企业信息，按重要性和持股比例排序
- 每个企业都应该有完整的信息（名称、是否上市、交易所、股票代码、持股比例、关系类型）
- relationship_type说明：
  - direct_parent: 直接控股母公司
  - indirect_parent: 间接控股母公司
  - ultimate_parent: 最终控制人
  - direct_subsidiary: 直接控股子公司
  - indirect_subsidiary: 间接控股子公司
  - sister_company: 兄弟公司（同一母公司控制）
  - joint_venture: 合资公司
  - associate_company: 参股公司
  - other: 其他关联关系
- 重点关注上市公司及其重要的非上市关联企业
- 示例：A公司由B公司控股60%，B公司由C公司控股80%，A公司控股D公司70%，A公司与E公司为兄弟公司，则应分别输出B、C、D、E公司的详细信息
"""
