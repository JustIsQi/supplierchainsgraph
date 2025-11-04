import json 
from openai import OpenAI
from datetime import datetime
prompt= """
Given the 【Schema】of NebulaGraph and the 【Question】, generate a **syntactically correct nGQL (NebulaGraph Query Language) query**.

【Important】NebulaGraph uses nGQL, which is different from Neo4j's Cypher. You MUST follow NebulaGraph-specific syntax rules.

You can write the answer in script blocks like this:
```ngql
MATCH (v:Player)-[e:Serve]->(t:Team)
WHERE id(v) == "player100"
RETURN v.Player.name AS PlayerName, t.Team.name AS TeamName;
```

【NebulaGraph-Specific Syntax Rules】

1. Property Access (CRITICAL):
   - Properties MUST use the format: `variable.TagName.propertyName` or `edge.propertyName`
   - ✓ Correct: `v.Company.company_name`, `e.sales_amount`
   - ✗ Wrong: `v.company_name`(missing tag type)

2. Query Statement Types:
   NebulaGraph supports multiple query patterns:
   
   a) MATCH (Pattern Matching):
      - Use for complex graph pattern matching
      - Must start from indexed properties or known vertex IDs
      - Example: `MATCH (n:Company) WHERE n.Company.company_name == "腾讯" RETURN n`
   
   b) GO (Graph Traversal - NebulaGraph Native):
      - Efficient for multi-hop traversal from known starting points
      - Syntax: `GO [n] STEPS FROM <vid> OVER <edge_type> [WHERE <conditions>] YIELD <properties>`
      - Example: `GO FROM "company1" OVER Subsidiary YIELD dst(edge) AS subsidiary_id`
   
   c) LOOKUP (Index-Based Query):
      - Use when querying by indexed properties
      - Syntax: `LOOKUP ON <tag> WHERE <tag>.<property> == <value> YIELD id(vertex) AS id`
      - Example: `LOOKUP ON Company WHERE Company.stock_code == "600000"`
   
   d) FETCH (Property Retrieval):
      - Get properties of specific vertices or edges
      - Syntax: `FETCH PROP ON <tag> <vid> YIELD properties(vertex)`
      - Example: `FETCH PROP ON Company "company123" YIELD properties(vertex).company_name`

3. Vertex ID (VID) Usage:
   - Use `id(vertex)` to get vertex ID
   - VID is mandatory when using GO or FETCH statements
   - Can combine LOOKUP to get VID first, then use GO for traversal

4. Edge Direction:
   - Outgoing edge: `-[e:EdgeType]->`
   - Incoming edge: `<-[e:EdgeType]-`
   - Bidirectional: `-[e:EdgeType]-`

5. Multi-Label Vertices:
   - NebulaGraph supports vertices with multiple tags
   - Specify tag explicitly when accessing properties
   - Example: A vertex can be both `Person` and `Shareholder`

6. Filtering and Conditions:
   - Use WHERE clause with fully qualified property names
   - Comparison operators: ==, !=, >, <, >=, <=, IN, CONTAINS, STARTS WITH
   - Logical operators: AND, OR, NOT
   - Example: `WHERE v.total_assets > 1000000000 AND v.industry == "科技"`

7. Return and Yield:
   - MATCH uses RETURN clause
   - GO, LOOKUP, FETCH use YIELD clause
   - Always use aliases for clarity: `AS alias_name`
   - Can use functions: `properties(vertex)`, `src(edge)`, `dst(edge)`, `type(edge)`

8. Aggregation and Ordering:
   - Aggregation functions: COUNT(), SUM(), AVG(), MAX(), MIN()
   - ORDER BY restrictions:
     * For vertex properties: Use fully qualified names: `ORDER BY v.TagName.property DESC`
     * For edge properties: CANNOT use `e.property` directly in ORDER BY
     * MUST first extract edge property as alias in RETURN/YIELD, then use alias in ORDER BY
   - Correct examples:
     * Vertex: `ORDER BY v.Company.total_assets DESC LIMIT 10`
     * Edge: `MATCH (v1:Company)-[e:Supplier]->(v2:Company) RETURN e.sales_amount AS amount ORDER BY amount DESC`
     * Edge with GO: `GO FROM "vid" OVER Supplier YIELD e.sales_amount AS amount ORDER BY amount DESC`
   - Wrong example: `ORDER BY e.sales_amount DESC` (will fail)

9. Schema Compliance:
   - ONLY use tags, edge types, and properties defined in the schema
   - Check edge start/end node types from schema
   - Respect data types (string, int64, float, bool)

10. Common Patterns:
    - Find by property: Use MATCH with WHERE or LOOKUP
    - Multi-hop traversal: Use GO with STEPS
    - Get neighbors: `GO FROM vid OVER edge_type YIELD dst(edge)`
    - Path finding: Use MATCH with pattern or FIND PATH statements

【Output Requirements】
- Respond with ONLY the nGQL query inside a ```ngql code block
- Do NOT include explanations, comments, or additional text
- Ensure the query is executable in NebulaGraph
- Use proper indentation for readability

【Schema】
{schema_info}

【Question】
{query_str}
"""

def claude_chat(message):  
    client = OpenAI(
        api_key="Bearer sk-9AiXl4JTI3FCPUIAkEh0Yw", # 在这里将 MOONSHOT_API_KEY 替换为你从 Kimi 开放平台申请的 API Key
        base_url="http://llmserver.rt-private-cloud.com/v1",
    )
    try:
        result = client.chat.completions.parse(
            model="anthropic/claude-opus-4-1-20250805-thinking",
            messages=[
                {"role":"user","content":message}
            ],
            temperature=0,
            max_completion_tokens=32768,
            timeout=600
        )

        return result.choices[0].message.content
    except Exception as e:
        print(f"API调用错误: {e}")
        return None

query_list = [
    '"徕木股份"来自汽车电子领域的收入占其总收入的比例是多少？该业务是否是其核心增长引擎？',
    '查询所有半导体公司中毛利率大于10%的公司',
    '审核比亚迪与其控股公司及其关联公司的供应链关系是否存在风险',
    '对比"宁德时代"和"比亚迪"的共同供应商，分析供应链重叠度',
    '查询"宜宾五粮液股份有限公司"的供应商中，哪些与其股东或高管存在关联关系？',
    '查询"腾讯"和"京东"之间是否存在交叉持股或供应链关系？',
    '查询对单一客户销售占比超过50%的A股上市公司，评估客户集中度风险',
    '查询"宁德时代"的客户中，哪些同时也采购其上游供应商的产品？',
    '查询"闻泰科技"收购的子公司"安世半导体"的供应链关系',
    '查询"长城汽车"和"吉利汽车"的供应商重叠情况，是否存在同业竞争？',
    '查询"小米集团"不同业务板块的供应商是否存在重叠',
    '对比"宁德时代"不同报告期的客户数量变化',
    '查询"比亚迪"的前十大供应商中，哪些同时也是上市公司',
    '对比"华为"不同主营产品的供应商分布情况',
    '评估"蔚来汽车"对单一供应商的依赖度，是否有替代方案？',
    '交叉验证"立讯精密"对"苹果公司"销售额，与苹果公告的采购额是否一致？',
    '查询"比亚迪"在不同报告期的前五大供应商变化情况',
    '在"比亚迪-宁德时代"供应关系中，谁掌握更强的议价能力？',
    '查询"闻泰科技"通过并购获得的子公司，分析其供应链关系',
    '查询"比亚迪"的二级供应商（供应商的供应商）有哪些？',
    '在新能源汽车行业中，统计前五大供应商的供货企业数量',
    '查询依赖单一供应商采购占比超过30%的上市公司',
    '查询"比亚迪"的供应商中，供应内容包含"电池"的供应商名单',
    '查询注册地在长三角地区（江浙沪）的新能源汽车产业链企业',
    '查询公告供应内容中提及"芯片"的供应商及其客户',
    '查询"中国石油"向其关联公司的采购和销售总金额',
    '查询半导体行业公司的供应商所属行业分布',
    '查询"理想汽车"的采购总额与营业收入的比例',
    '查询"宁德时代"的客户所属行业分布情况',
    '查询"比亚迪"的股东中，哪些股东控制的企业也是其供应商',
    '查询"特斯拉"在中国的供应商注册地分布',
    '查询在多家新能源汽车公司任职的高管',
    '对比"格力电器"向关联公司和非关联公司的销售金额占比',
    '查询新能源汽车产业链中，各零部件类别的供应商数量',
    '查询具有ISO9001资质的公司作为供应商的下游客户覆盖',
    '查询"京东方"的经营地分布情况',
    '查询"美的集团"的关联公司网络（2跳）',
    '对比"京东方"不同报告期的营业收入增长率',
    '查询"格力电器"的大股东（持股>5%）控制的企业，是否也是该公司的供应商或客户？',
    '追踪"理想汽车"不同报告期前五大供应商的采购集中度',
    '查询"海尔智家"与其关联公司的关联关系类型及数量',
    '查询"宁德时代"与主要客户的合作报告期数量',
    '对比家电行业公司的毛利率分布，识别异常值',
    '查询"海信家电"各业务板块的营业成本和毛利对比',
    '查询"比亚迪"的供应商注册地分布',
    '查询"小米"不同业务类型的供应商数量对比',
    '查询"万科集团"的子公司作为独立主体的供应链关系',
    '查询"中国平安"的股东中，持股比例与投票权比例不一致的情况（一致行动人识别）',
    '对比光伏行业不同企业的毛利率和营业收入规模',
    '对比"蔚来/理想/小鹏"三家新势力的供应商数量',
    '比亚迪的客户公司及其采购金额',
    '查询"宁德时代"是否通过供应链间接与"比亚迪"有关联',
    '查询"ZHIXUZHOU"作为股东的公司，以及这些公司是否也是"华大半导体"的供应商',
    '查询公司(麦趣尔集团股份有限公司)的所有一级股东中，哪些是自然人，并列出其职位',
    '查询"宁德时代"的前五大供应商名称及供货金额',
    '查询"比亚迪"在最近报告期的前十大客户公司及销售金额',
    '查询"华为技术有限公司"的主营产品名称及收入占比',
    '查询"小米集团"的现任董事会成员名单',
    '查询持有"格力电器"股份超过5%的股东',
    '查询"比亚迪"向"宁德时代"的采购总额',
    '查询"立讯精密"的前五大客户公司及销售金额',
    '查询"阿里巴巴"的董事长和总经理姓名',
    '查询"腾讯控股"的所属行业和企业类型',
    '查询"比亚迪"各业务板块的主营产品名称及收入占比',
    '查询"蔚来汽车"当前的前十大供应商名称及采购金额',
    '查询"中芯国际"的前十大客户公司',
    '查询"美的集团"的全资子公司列表',
    '查询与"隆基绿能"同属于光伏行业的上市公司',
    '查询"恒大集团"状态为离职的高管名单',
    '查询"贵州茅台"的A股股票代码',
    '查询"五粮液"的前三大股东及持股比例',
    '查询"小米集团"的显示屏供应商（基于供应内容模糊匹配）',
    '查询"宁德时代"的锂矿原料供应商（基于供应内容）',
    '查询"海康威视"各业务板块的营业收入及占比',
    '查询"比亚迪"2023年主营业务收入及毛利率',
    '查询"比亚迪"全体高管的薪酬总额',
    '查询"中芯国际"最近报告期的主营业务收入',
    '查询"宁德时代"向"理想汽车"的供货金额',
    '查询"药明康德"公告披露的公司资质类型',
    '查询"理想汽车"的上市时间及上市交易所',
    '查询"紫光国微"所属的证监会行业分类',
    '查询"三一重工"的主营业务产品类别',
    '查询"中国平安"2023年的主营业务毛利率',
    '查询"恒大集团"旗下主要子公司名称及持股比例',
    '查询"乐视网"的关联公司名单及关联关系',
    '查询"腾讯控股"对外投资的子公司及持股比例',
    '查询"蔚来汽车"最近报告期股东持股数量变动情况（增减）',
    '查询"小米集团"公告披露的代工供应商（基于供应内容）',
    '查询"京东方"各业务板块的收入占比',
    '查询"比亚迪"向前五大供应商的采购占比',
    '查询"格力电器"的股票上市状态及风险警示情况',
    '查询"紫金矿业"对子公司的投资方式（股权转让/收购兼并等）',
    '查询"京东方"2023年各业务板块的营业收入',
    '查询"万科集团"主要子公司的名称及持股比例',
    '查询"长城汽车"的主营产品名称及收入占比',
    '查询"贵州茅台股份"前五大客户的销售金额',
    '查询"比亚迪"公告披露的主要经营地和注册地',
    '查询"上汽集团"的非全资子公司及持股比例',
    '查询"中国石油"的独立董事名单',
    '查询"中国移动"公告披露的公司资质'
]

with open('./YXSupplyChains_desc.json', 'r', encoding='utf-8') as f:
    schema_info = json.load(f)

# 测试单个查询
cypher_list = {}
for query_str in query_list:
   try:
      cypher_prompt = prompt.format(schema_info=schema_info, query_str=query_str)
      print(f'{datetime.now()} 开始查询: {query_str}')
      response = claude_chat(cypher_prompt)
      print(f"查询: {query_str}")
      print(f"\n生成的nGQL:\n{response}")
      if response is None:
         continue
      cypher_list[query_str] = response.replace('```ngql', '').replace('```', '').replace('\n', '')
   except:
      pass
with open('./YXSupplyChains_cypher.json', 'w', encoding='utf-8') as f:
    json.dump(cypher_list, f, ensure_ascii=False, indent=4)