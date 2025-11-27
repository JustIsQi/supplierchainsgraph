import json


output_schema = json.dumps({
  "Products": {
    "description": "Company's main product line categories",
    "iPhone": {
      "description": "Smartphone product line based on iOS operating system",
      "value": 201183,
      "currency": "USD",
      "unit": "million",
      "growth_rate": 0.05
    },
    "Mac": {
      "description": "Personal computer product line based on macOS operating system",
      "value": 29357,
      "currency": "USD",
      "unit": "million",
      "growth_rate": -0.27
    }
  },
  "Services": {
    "description": "Various service businesses provided by the company",
    "Advertising": {
      "description": "Advertising services including third-party licensing arrangements and company-owned advertising platforms",
      "value": 5678,
      "currency": "USD",
      "unit": "million"
    }
  },
  "Platform": {
    "description": "Revenue data classified by application platform",
    "High Performance Computing": {
      "description": "Revenue related to high-performance computing platform",
      "value": 1476891,
      "currency": "TWD",
      "unit": "thousand"
    },
    "Smartphone": {
      "description": "Revenue related to smartphone platform",
      "value": 1005130,
      "currency": "TWD",
      "unit": "thousand",
      "growth_rate": 0.12
    }
  },
  "Resolution": {
    "description": "Revenue proportion classified by technology process node",
    "3-nanometer": {
      "description": "Revenue proportion of 3-nanometer process technology",
      "ratio": 0.18
    },
    "5-nanometer": {
      "description": "Revenue proportion of 5-nanometer process technology",
      "ratio": 0.34
    }
  }
}, indent=2)

OVERSEA_STUDY_PROMPT = """
You are a professional financial data analyst who needs to extract production, operations, and revenue-related data from financial reports of U.S. listed companies (such as 10-K, 20-F, annual reports). Please follow the requirements below:

Task Description:
Carefully read the entire report, focusing on sections such as "Business Description", "Revenue Composition", "Operating Segments", "Products and Services", etc.

Analyze Data Scope and Hierarchical Structure:
Different companies may have completely different data hierarchies. You need to identify all levels of classification dimensions (e.g., Level 1 → Level 2 → Level 3 → Specific Business/Product).
Example hierarchies may include:
- Production and Operations: Product lines (e.g., iPhone, Mac), service types (e.g., advertising, cloud services), technology nodes (e.g., 3-nanometer, 5-nanometer), business divisions, etc.
- Revenue: Platform types (e.g., HPC, smartphones), geographic regions, technology processes, customer types, etc.
If the report explicitly lists multiple levels (e.g., Level 1, Level 2, Level 3), extract all of them.

Extraction Content:
1. Indicator names at each level (e.g., "Products", "iPhone", "High Performance Computing").
2. Each indicator must include a summary description ("description" field) to explain the business meaning, classification dimension, or data scope of the indicator. The description should be concise and clear, helping to understand the indicator's position and role in the business.
3. Corresponding data values, which should be extracted as follows:
   - "value": ONLY the numeric value (e.g., 201183, 1476891). Do not include currency symbols or unit descriptions in this field.
   - "currency": The currency code mentioned in the original text (e.g., "USD", "EUR", "TWD", "CNY"). Extract exactly as stated in the document.
   - "unit": The magnitude unit of the numeric value (e.g., "million", "billion", "thousand", "hundred million"). Extract exactly as stated in the document.
   - "growth_rate": If growth rate or year-over-year change is mentioned, extract it as a decimal number (e.g., 0.05 for 5% growth, -0.27 for -27% decline). This field is optional.
   - "ratio": For percentage or proportion data that represents composition rather than growth (e.g., market share, revenue mix), extract as a decimal (e.g., 0.18 for 18%).
4. For descriptive business content (e.g., product features, service scope), if no numeric value exists, you may include the description in the "value" field.

CRITICAL REQUIREMENT - Data Extraction:
ALL extracted data fields MUST come directly from the original text of the report:
- "value": Extract the exact numeric value as it appears in the text. Do not perform calculations or conversions.
- "currency": Extract the exact currency code or symbol mentioned in the original text (e.g., if the text says "USD millions", extract "USD").
- "unit": Extract the exact magnitude unit mentioned in the original text (e.g., "million", "billion", "千", "百万"). Preserve the original language if necessary.
- "growth_rate": Extract the exact growth/change percentage mentioned and convert to decimal format (e.g., "increased 5%" becomes 0.05, "decreased 10%" becomes -0.10).
- For descriptive content without numeric values, preserve the original wording from the source document in the "value" field.
Do not introduce external knowledge or make inferences beyond what is explicitly stated in the document.


Output Format:
Output in JSON format. The structure should be a multi-level nested object that reflects the hierarchical relationships in the actual report.
Each node (including classification nodes and leaf nodes) should contain:
- "description": Summary description (required), explaining the business meaning, classification dimension, or data scope of the indicator
- "value": Numeric data value only (number). If a level has no data value and is only a classification node, this field may be omitted. For descriptive content without numeric values, this field may contain the text description.
- "currency": Currency code (optional, e.g., "USD", "EUR", "TWD"). Only include when a numeric value is present.
- "unit": Magnitude unit (optional, e.g., "million", "billion", "thousand"). Only include when a numeric value is present.
- "growth_rate": Growth rate as decimal (optional, e.g., 0.05 for 5%, -0.10 for -10%). Only include when growth/change data is mentioned.
- "ratio": Proportion as decimal (optional, e.g., 0.18 for 18%). Use this for composition data like market share or revenue mix.
Note: Even if a level is only a classification node, it must provide a "description" field to explain its classification meaning.

Example Reference (based on the provided file):
{output_schema}

Important Notes:
1. If the same company has multiple classification dimensions in the same report (e.g., "Platform" and "Resolution"), list them as parallel levels.
2. Separate numeric data into distinct fields: "value" (numeric only), "currency" (currency code), "unit" (magnitude unit), and "growth_rate" (if applicable).
3. For descriptive business content without numeric values, preserve the key information from the original text in the "value" field and avoid summarization.
4. Ensure that all extracted fields come from the report body and do not introduce external knowledge.
5. ALL numeric values, currency codes, and units MUST be verbatim extracts from the original document text. Do not perform conversions, calculations, or modify the original data representation.

Please process the input company report according to the above rules and output compliant JSON.
{documents}
"""