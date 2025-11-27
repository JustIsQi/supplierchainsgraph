import json


output_schema = json.dumps({
  "Products": {
    "description": "Company's main product line categories",
    "iPhone": {
      "description": "Smartphone product line based on iOS operating system",
      "value": "The Company\\'s line of smartphones based on its iOS operating system..."
    },
    "Mac": {
      "description": "Personal computer product line based on macOS operating system",
      "value": "The Company\\'s line of personal computers based on its macOS® operating system..."
    }
  },
  "Services": {
    "description": "Various service businesses provided by the company",
    "Advertising": {
      "description": "Advertising services including third-party licensing arrangements and company-owned advertising platforms",
      "value": "5678"
    }
  },
  "Platform": {
    "description": "Revenue data classified by application platform",
    "High Performance Computing": {
      "description": "Revenue related to high-performance computing platform",
      "value": 1476891,
      "unit": "TWD"
    },
    "Smartphone": {
      "description": "Revenue related to smartphone platform",
      "value": 1005130,
      "unit": "TWD"
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
3. Corresponding data values, which may be:
   - Numeric values (e.g., revenue amounts, proportions)
   - Units (e.g., TWD, millions)
   - Descriptive business content (e.g., product features, service scope)
4. If "proportion" (percentage) exists, extract it as well.

CRITICAL REQUIREMENT - Value Extraction:
ALL "value" fields MUST be extracted directly from the original text of the report. Do not paraphrase, summarize, or modify the original wording. If the value is a number, use the exact number from the text. If the value is descriptive text, preserve the original wording from the source document. Do not introduce external knowledge or make inferences beyond what is explicitly stated in the document.


Output Format:
Output in JSON format. The structure should be a multi-level nested object that reflects the hierarchical relationships in the actual report.
Each node (including classification nodes and leaf nodes) should contain:
- "description": Summary description (required), explaining the business meaning, classification dimension, or data scope of the indicator
- "value": Data value (number, string, or object). If a level has no data value and is only a classification node, this field may be omitted
- Optional: "unit" (unit), "ratio" (proportion)
Note: Even if a level is only a classification node, it must provide a "description" field to explain its classification meaning.

Example Reference (based on the provided file):
{output_schema}

Important Notes:
1. If the same company has multiple classification dimensions in the same report (e.g., "Platform" and "Resolution"), list them as parallel levels.
2. If business data is descriptive text, preserve the key information from the original text and avoid summarization.
3. Ensure that all extracted fields come from the report body and do not introduce external knowledge.
4. ALL "value" fields MUST be verbatim extracts from the original document text. Do not modify, paraphrase, or summarize the original wording.

Please process the input company report according to the above rules and output compliant JSON.
{documents}
"""