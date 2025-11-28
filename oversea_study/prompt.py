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
You are a professional financial data analyst specializing in extracting structured production, operations, and revenue-related data from financial reports of U.S. listed companies (such as 10-K, 20-F, annual reports).

TASK OBJECTIVE:
Extract all production, operations, and revenue data from the provided document and organize them into a clear, unified tree-structured hierarchy that reflects the company's business logic and reporting structure.

---

HIERARCHICAL STRUCTURE RULES:

1. LEVEL DEFINITION:
   - Level 1: Top-level business dimensions (e.g., "Business Segments", "Revenue Composition", "Geographic Distribution", "Product Categories")
   - Level 2: Major categories within each dimension (e.g., "North America", "iPhone", "Services")
   - Level 3: Sub-categories or detailed metrics (e.g., "Net Sales", "Operating Income", "Advertising Services")
   - Level 4+: Further breakdowns if present (e.g., "Cost of Sales", "Fulfillment Expenses")

2. STRUCTURE REQUIREMENTS:
   - Use ONLY nested objects to represent parent-child relationships
   - Each level must have a clear semantic meaning (what dimension it represents)
   - Sibling nodes at the same level should represent the same type of classification
   - NEVER mix different classification dimensions at the same level
   - Each node must contain either child nodes OR data values, but clearly distinguish between category nodes and data leaf nodes

3. NODE TYPES:
   a) Category Node (has children): Contains "description" + child nodes
   b) Data Leaf Node: Contains "description" + data fields (value, currency, unit, growth_rate, ratio)

---

DATA EXTRACTION RULES:

1. FIELD DEFINITIONS:
   - "description": (REQUIRED for ALL nodes) Clear explanation of what this node represents
     * For category nodes: describe the classification dimension or business scope
     * For data nodes: describe what metric this data represents
   
   - "value": (OPTIONAL, numeric only) The exact numeric value from the document
     * Extract as pure number (e.g., 201183, not "$201,183M")
     * Do NOT perform any calculations or conversions
     * Only include if the document explicitly provides this number
   
   - "currency": (OPTIONAL) Currency code exactly as stated (e.g., "USD", "EUR", "TWD", "CNY")
   
   - "unit": (OPTIONAL) Magnitude unit exactly as stated (e.g., "million", "billion", "thousand")
   
   - "growth_rate": (OPTIONAL) Growth/change rate as decimal
     * Positive for growth (e.g., "increased 5%" → 0.05)
     * Negative for decline (e.g., "decreased 10%" → -0.10)
   
   - "ratio": (OPTIONAL) Composition/proportion as decimal (e.g., "18% of total" → 0.18)
     * Use for market share, revenue mix, or percentage breakdowns
     * Do NOT use for growth rates

2. CRITICAL REQUIREMENTS:
   - Extract ONLY from the provided document text - NO external knowledge
   - Do NOT calculate, convert, or infer any values
   - Preserve original currency and units as stated in the document
   - If data is missing or unclear, OMIT that field rather than guessing
   - VERIFY each number against the source text before including it

---

OUTPUT FORMAT:

Return a JSON object with the following structure:

{
  "Level1_Dimension_Name": {
    "description": "Explanation of this business dimension",
    "node_type": "category",
    "Level2_Category_Name": {
      "description": "Explanation of this category",
      "node_type": "category",
      "Level3_Metric_Name": {
        "description": "Explanation of this specific metric",
        "node_type": "data",
        "value": <number>,
        "currency": "<string>",
        "unit": "<string>",
        "growth_rate": <decimal>,
        "ratio": <decimal>
      }
    }
  }
}

EXAMPLE STRUCTURE:
{output_schema}

---

IMPORTANT NOTES:

1. CONSISTENCY: All nodes at the same depth under the same parent should represent the same type of classification

2. COMPLETENESS: Extract ALL relevant production, operations, and revenue data mentioned in the document

3. ACCURACY: Every data value must be traceable to a specific statement in the source document

4. CLARITY: The hierarchy should be immediately understandable - a reader should be able to navigate from high-level business overview down to specific metrics

5. NO DUPLICATION: Each piece of data should appear only once in the tree at its most appropriate location

6. FOCUS AREAS: Pay special attention to:
   - Business segment breakdowns
   - Revenue composition by product/service/geography
   - Operating metrics and KPIs
   - Cost structure and expense breakdowns
   - Year-over-year comparisons

---

Now process the following financial report and extract the structured data:

{documents}
"""