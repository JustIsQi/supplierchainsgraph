import json


output_schema = json.dumps({
  "Products": {
    "description": "公司主要产品线分类",
    "iPhone": {
      "description": "基于iOS操作系统的智能手机产品线",
      "value": "The Company\\'s line of smartphones based on its iOS operating system..."
    },
    "Mac": {
      "description": "基于macOS操作系统的个人电脑产品线",
      "value": "The Company\\'s line of personal computers based on its macOS® operating system..."
    }
  },
  "Services": {
    "description": "公司提供的各类服务业务",
    "Advertising": {
      "description": "包括第三方许可安排和公司自有广告平台的广告服务",
      "value": "5678"
    }
  },
  "Platform": {
    "description": "按应用平台分类的营业收入数据",
    "High Performance Computing": {
      "description": "高性能计算平台相关的营业收入",
      "value": 1476891,
      "unit": "TWD"
    },
    "Smartphone": {
      "description": "智能手机平台相关的营业收入",
      "value": 1005130,
      "unit": "TWD"
    }
  },
  "Resolution": {
    "description": "按技术制程节点分类的收入占比",
    "3-nanometer": {
      "description": "3纳米制程技术的收入占比",
      "ratio": 0.18
    },
    "5-nanometer": {
      "description": "5纳米制程技术的收入占比",
      "ratio": 0.34
    }
  }
}, indent=2)

OVERSEA_STUDY_PROMPT = """
你是一个专业金融数据分析师，需要从美股上市公司的财务公告(如10-K、20-F、年度报告)中提取生产经营和营业收入相关数据。请遵循以下要求:

任务说明:
仔细阅读公告全文，重点关注"业务描述""收入构成""经营分部""产品与服务"等章节。

分析数据的口径与层级结构:
不同公司的数据层级可能完全不同，需识别所有层级的分类维度(如:一级分类 → 二级分类 → 三级分类 → 具体业务/产品)。
示例层级可能包括:
- 生产经营:产品线(如iPhone、Mac)、服务类型(如广告、云服务)、技术节点(如3纳米、5纳米)、业务部门等。
- 营业收入:平台类型(如HPC、智能手机)、地理区域、技术制程、客户类型等。
如果公告中明确列出了多个层级(如一级、二级、三级)，请全部提取。

提取内容:
1. 每一层级的指标名称(如"Products"、"iPhone"、"High Performance Computing")。
2. 每个指标必须包含一个概括型描述("description"字段)，用于说明该指标的业务含义、分类维度或数据口径。描述应简洁明了，能够帮助理解该指标在业务中的定位和作用。
3. 对应的数据值，可能是:
   - 数值(如收入金额、占比)
   - 单位(如TWD、百万)
   - 描述性业务内容(如产品功能、服务范围)
4. 如果存在"比重"(百分比)，请一并提取。

忽略以下字段:股票代码、公司名称、财年、ID、数据来源(除非数据本身嵌入在业务描述中)。

输出格式:
使用JSON格式输出。结构应为多层级嵌套对象，反映实际公告中的层级关系。
每个节点(包括分类节点和叶子节点)应包含:
- "description":概括型描述(必需)，说明该指标的业务含义、分类维度或数据口径
- "value":数据值(数字、字符串或对象)，如果某层级无数据值，仅作为分类节点，可省略此字段
- 可选:"unit"(单位)、"ratio"(比重)
注意:即使某层级仅作为分类节点，也必须提供"description"字段来说明其分类意义。

示例参考(基于你提供的文件):
{output_schema}

注意事项:
1. 如果同一公司在同一公告中有多个分类维度(如"Platform"和"Resolution")，请分别列为并列层级。
2. 若业务数据为描述性文本，请保留原文关键信息，避免摘要。
3. 确保所有提取字段均来自公告正文，不引入外部知识。

请根据上述规则，处理输入的公司公告，并输出合规的JSON。
{documents}
"""