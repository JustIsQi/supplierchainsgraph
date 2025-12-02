import json


output_schema = json.dumps({
  "Business_Segments": {
    "description": "Revenue and growth data by business segment across multiple time periods",
    "node_type": "category",
    "North_America": {
      "description": "North America segment net sales",
      "node_type": "category",
      "Net_Sales": {
        "2022": {
          "description": "Net sales for North America segment in 2022",
          "node_type": "data",
          "value": "$ 315,880",
          "currency": "USD",
          "unit": "million",
          "time_period": "2022"
        },
        "2023": {
          "description": "Net sales for North America segment in 2023",
          "node_type": "data",
          "value": "$ 352,828",
          "currency": "USD",
          "unit": "million",
          "time_period": "2023"
        }
      },
      "Year_over_year_Growth": {
        "2022": {
          "description": "Year-over-year percentage growth for 2022",
          "node_type": "data",
          "value": "13%",
          "time_period": "2022"
        },
        "2023": {
          "description": "Year-over-year percentage growth for 2023",
          "node_type": "data",
          "value": "12%",
          "time_period": "2023"
        }
      }
    },
    "AWS": {
      "description": "Amazon Web Services segment",
      "node_type": "category",
      "Net_Sales": {
        "2022": {
          "description": "AWS net sales in 2022",
          "node_type": "data",
          "value": "80,096",
          "currency": "USD",
          "unit": "million",
          "time_period": "2022"
        },
        "2023": {
          "description": "AWS net sales in 2023",
          "node_type": "data",
          "value": "90,757",
          "currency": "USD",
          "unit": "million",
          "time_period": "2023"
        }
      },
      "Growth_Rate": {
        "2022": {
          "description": "Year-over-year growth rate for 2022",
          "node_type": "data",
          "value": "29",
          "time_period": "2022"
        },
        "2023": {
          "description": "Year-over-year growth rate for 2023",
          "node_type": "data",
          "value": "13",
          "time_period": "2023"
        }
      }
    }
  },
  "Revenue_Mix": {
    "description": "Net sales mix percentages by segment",
    "node_type": "category",
    "North_America_Mix": {
      "2022": {
        "description": "North America proportion of total revenue in 2022",
        "node_type": "data",
        "value": "61%",
        "time_period": "2022"
      },
      "2023": {
        "description": "North America proportion of total revenue in 2023",
        "node_type": "data",
        "value": "61%",
        "time_period": "2023"
      }
    },
    "AWS_Mix": {
      "2022": {
        "description": "AWS proportion of total revenue in 2022",
        "node_type": "data",
        "value": "16%",
        "time_period": "2022"
      },
      "2023": {
        "description": "AWS proportion of total revenue in 2023",
        "node_type": "data",
        "value": "16%",
        "time_period": "2023"
      }
    }
  }
}, indent=2)

OVERSEA_STUDY_PROMPT = """
You are a professional financial data analyst specializing in extracting structured production, operations, and revenue-related data from financial reports of U.S. listed companies (such as 10-K, 20-F, annual reports).

TASK OBJECTIVE:
Extract all production, operations, and revenue data from the provided document and organize them into a clear, unified tree-structured hierarchy that reflects the company's business logic and reporting structure.

**IMPORTANT: EXCLUDE ALL FINANCIAL STATEMENT DATA**
- DO NOT extract data from the following standard financial statement sections:
  * Condensed Consolidated Statements of Operations
  * Condensed Consolidated Statements of Comprehensive Income
  * Condensed Consolidated Statements of Cash Flows
  * Condensed Consolidated Statements of Stockholders' Equity
  * Consolidated Balance Sheets
  * Consolidated Statements of Income
  * Consolidated Statements of Cash Flows
- ONLY focus on: Business segment reporting, Product line performance, Revenue breakdowns by category/geography/customer, Production/manufacturing data, Operating metrics, Market analysis

**WHAT TO EXTRACT (Examples):**
✓ "iPhone revenue was $200 billion, up 5% year-over-year"
✓ "North America segment generated $120 billion in net sales"
✓ "Data Center revenue increased 112% to $30.8 billion"
✓ "Manufacturing output reached 2.5 million units in Q3"
✓ "Cloud services accounted for 32% of total revenue"

**WHAT TO IGNORE (Examples):**
✗ Balance sheet line items (Total Assets, Current Liabilities, Property & Equipment)
✗ Comprehensive income statement totals (Net Income, Operating Income from consolidated statements)
✗ Cash flow activities (Operating/Investing/Financing cash flows)
✗ Standard accounting entries not tied to specific business segments or products

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
   
   - "value": (OPTIONAL, string or numeric) The exact value from the document in its ORIGINAL FORMAT
     * Keep percentages as strings with "%" symbol (e.g., "13%", "11%", "56.2%")
     * Keep numbers with commas as strings (e.g., "315,880", "1,234")
     * Keep negative numbers in parentheses if that's how they appear (e.g., "(8)", "(1,234)")
     * Do NOT convert to decimals or remove formatting
     * Do NOT perform any calculations or conversions
     * Only include if the document explicitly provides this value
   
   - "currency": (OPTIONAL) Currency code exactly as stated (e.g., "USD", "EUR", "TWD", "CNY")
   
   - "unit": (OPTIONAL) Magnitude unit exactly as stated (e.g., "million", "billion", "thousand")
   
   - "time_period": (OPTIONAL) Time period for the data point (e.g., "2023", "Q3 2023", "Year Ended December 31, 2023")
     * CRITICAL: When a table or section contains data for MULTIPLE time periods (years, quarters, etc.), you MUST extract data for ALL time periods
     * Create separate data nodes for each time period to preserve complete time series data
     * Example: If a table shows "2022: $315,880" and "2023: $352,828", create TWO separate nodes with corresponding time_period fields

2. CRITICAL REQUIREMENTS:
   - Extract ONLY from the provided document text - NO external knowledge
   - Do NOT calculate, convert, or infer any values
   - Preserve original currency and units as stated in the document
   - **PRESERVE ORIGINAL FORMAT**: Keep all values EXACTLY as they appear in the source
     * Percentages: Keep as "13%", "11%", "(8)" - DO NOT convert to 0.13, 0.11, -0.08
     * Large numbers: Keep as "315,880" or 315880 depending on source format
     * Negative values: Keep as "(8)" if that's the original format
   - **MULTI-PERIOD DATA EXTRACTION**: When tables or sections show data across multiple time periods:
     * Extract data for ALL time periods present (e.g., 2022, 2023, Q1, Q2, etc.)
     * Create separate data nodes for each time period with matching metrics
     * Ensure one-to-one correspondence between metrics across different periods
     * Use "time_period" field to distinguish between different time points
   - If data is missing or unclear, OMIT that field rather than guessing
   - VERIFY each value against the source text before including it

3. TABLE DATA EXTRACTION RULES:
   - **PRESERVE ORIGINAL FORMAT - DO NOT CONVERT ANYTHING**:
     * Percentages: Keep as "6%", "56.2%", "(8)" - DO NOT convert to decimals
     * Numbers in parentheses: Keep as "(39)", "(1,234)" - DO NOT convert to negative numbers
     * Large numbers: Keep exactly as shown: "315,880" or 315880
     * Currency symbols: Keep as "$315,880" if present in original
   - **MULTI-PERIOD TABLE HANDLING**:
     * When a table has columns for different years/quarters (e.g., "2022" and "2023")
     * Extract data for EACH time period as separate nodes
     * Example structure for a table showing revenue across 2022 and 2023:
       ```
       "North_America_Revenue": {
         "2022": {"value": "$ 315,880", "currency": "USD", "unit": "million", "time_period": "2022"},
         "2023": {"value": "$ 352,828", "currency": "USD", "unit": "million", "time_period": "2023"}
       }
       ```
   - ALL values must be extracted EXACTLY as shown in the original document
     * Do NOT round, truncate, or modify any numbers
     * Do NOT interpret or convert values based on context or assumptions

---

OUTPUT FORMAT:

Return a JSON object with the following structure:

For single time period data:
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
        "value": "<string or number in ORIGINAL FORMAT>",
        "currency": "<string>",
        "unit": "<string>",
        "time_period": "<string>"
      }
    }
  }
}

For multi-period data (REQUIRED when source contains multiple time periods):
{
  "Level1_Dimension_Name": {
    "description": "Explanation of this business dimension",
    "node_type": "category",
    "Level2_Category_Name": {
      "description": "Explanation of this category",
      "node_type": "category",
      "Level3_Metric_Name": {
        "time_periods": {
          "description": "Time periods",
          "node_type": "category",
          "time_periods_1": {
            "description": "Time periods 1",
            "node_type": "data",
            "value": "<value in original format>",
            "time_period": "time periods 1"
          },
          "time periods 2": {
            "description": "Time periods 2",
            "node_type": "data",
            "value": "<value in original format>",
            "time_period": "time periods 2"
          },
          ...
          "time periods n": {
            "description": "Time periods n",
            "node_type": "data",
            "value": "<value in original format>",
            "time_period": "time periods n"
          }
        }
      }
    }
  }
}

---

IMPORTANT NOTES:

1. CONSISTENCY: All nodes at the same depth under the same parent should represent the same type of classification

2. COMPLETENESS: Extract ALL relevant production, operations, and revenue data mentioned in the document

3. ACCURACY: Every data value must be traceable to a specific statement in the source document

4. CLARITY: The hierarchy should be immediately understandable - a reader should be able to navigate from high-level business overview down to specific metrics

5. NO DUPLICATION: Each piece of data should appear only once in the tree at its most appropriate location

6. **MULTI-PERIOD DATA HANDLING (CRITICAL)**:
   - When tables or sections contain data for MULTIPLE time periods (years, quarters, etc.), you MUST extract ALL periods
   - Create separate child nodes for each time period (e.g., "2022", "2023", "Q1_2023", "Q2_2023")
   - Ensure one-to-one correspondence: if metric A has data for 2022 and 2023, metric B should also have both
   - Use consistent time period labels across all metrics in the same table
   - Example: If a table shows revenue for 2022 and 2023, extract BOTH years, not just the latest

7. **FORMAT PRESERVATION (CRITICAL)**:
   - Keep ALL values in their ORIGINAL format from the document
   - Percentages: "13%", "11%", "(8)" - NEVER convert to 0.13, 0.11, -0.08
   - Large numbers: "315,880" or "$315,880" - keep commas and currency symbols as shown
   - Negative values in parentheses: "(8)" - do NOT convert to -8
   - This applies to ALL data fields: value, growth_rate, ratio, etc.

8. FOCUS AREAS: Pay special attention to:
   - Business segment breakdowns and performance
   - Revenue composition by product/service/geography/customer
   - Product line sales, volume, and pricing
   - Manufacturing/production capacity and output
   - Operating metrics and business KPIs
   - Market share and competitive positioning
   - Year-over-year growth and trends
   
9. EXCLUDE: Do NOT extract data from:
   - Consolidated or parent company financial statements (Balance Sheet, Income Statement, Cash Flow)
   - General accounting line items (Total Assets, Total Liabilities, Cash and Equivalents, etc.)
   - Standard financial ratios not related to business operations

---

Now process the following financial report and extract the structured data:

{documents}
"""