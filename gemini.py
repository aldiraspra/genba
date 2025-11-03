from typing import TypedDict, Optional, Dict, List, Union, Any
import logging
import json
import os
import pandas as pd
import duckdb
import google.generativeai as genai
from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver
from dotenv import load_dotenv
import sys
from pathlib import Path
import re
from datetime import datetime

# Load environment variables
_ = load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Configure Gemini
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))

# Global cache for DuckDB connections (keyed by file_name)
_DUCKDB_CONNECTION_CACHE = {}
_REGISTERED_SHEETS_CACHE = {}

def get_or_create_duckdb_connection(file_name: str) -> tuple[duckdb.DuckDBPyConnection, bool]:
    """Get cached DuckDB connection or create new one
    
    Returns:
        tuple: (connection, is_new) where is_new indicates if sheets need to be registered
    """
    file_path = os.path.join(os.getcwd(), file_name)
    
    # Check if connection exists and sheets are already registered
    if file_name in _DUCKDB_CONNECTION_CACHE and file_name in _REGISTERED_SHEETS_CACHE:
        logger.info(f"♻️ Reusing cached DuckDB connection for {file_name}")
        return _DUCKDB_CONNECTION_CACHE[file_name], False
    
    # Create new connection
    logger.info(f"🆕 Creating new DuckDB connection for {file_name}")
    con = duckdb.connect()
    _DUCKDB_CONNECTION_CACHE[file_name] = con
    return con, True

def clear_duckdb_cache(file_name: str = None):
    """Clear cached DuckDB connections
    
    Args:
        file_name: If provided, clear only this file's cache. Otherwise clear all.
    """
    global _DUCKDB_CONNECTION_CACHE, _REGISTERED_SHEETS_CACHE
    
    if file_name:
        if file_name in _DUCKDB_CONNECTION_CACHE:
            _DUCKDB_CONNECTION_CACHE[file_name].close()
            del _DUCKDB_CONNECTION_CACHE[file_name]
        if file_name in _REGISTERED_SHEETS_CACHE:
            del _REGISTERED_SHEETS_CACHE[file_name]
        logger.info(f"🗑️ Cleared cache for {file_name}")
    else:
        for con in _DUCKDB_CONNECTION_CACHE.values():
            con.close()
        _DUCKDB_CONNECTION_CACHE.clear()
        _REGISTERED_SHEETS_CACHE.clear()
        logger.info("🗑️ Cleared all DuckDB caches")

def get_user_friendly_error_message() -> str:
    """Return a user-friendly error message instead of technical details"""
    return "Maaf, saya mengalami kesulitan memproses pertanyaan Anda. Mohon coba lagi atau formulasikan pertanyaan dengan cara yang berbeda."

# Prompts
QUERY_GENERATION_PROMPT = """You are an Excel analysis expert that generates SQL or Pandas queries to analyze data from multiple Excel sheets.

CURRENT DATE & TIME CONTEXT:
- Today's Date: {current_date}
- Current Month: {current_month}
- Current Year: {current_year}

When user says "this month", "today", "current month", etc., use the current date context above.
Example: If user asks "revenue this month" and today is October 2025, query for October 2025 data.

CONVERSATION MEMORY:
- You have access to previous conversation history (if any)
- Use conversation context to understand follow-up questions like "show me more", "what about last month?", "give me details"
- If user refers to previous results or asks comparative questions, use the conversation history to understand what they're referring to
- When answering follow-up questions, ensure your query builds upon or relates to previous queries when appropriate

IMPORTANT DATA FORMAT:
- **Revenue values in the data are in IDR (Indonesian Rupiah) in MILLIONS**
- Example: A value of 450 in the data means IDR 450 Million (IDR 450,000,000)
- When analyzing revenue, always remember values are already in millions

UNIT REVENUE STRUCTURE:
The dataset contains several revenue units, each representing different product categories:
1. **Revenue D-Max**
2. **Revenue mu-X**
3. **Revenue Traga**
4. **Revenue N-Series**
5. **Revenue N-Series 4ban**
6. **Revenue N-Series 6ban**
7. **Revenue F+G Series 4 X 2**
8. **Revenue F+G Series 6 X 2**
9. **Revenue F+G Series 6 X 4**
10. **Revenue F+G Series Tractor Head**
- All these units can be found in relevant sheets.
- For **overall total revenue**, refer to the **sheet "Financial Performance"** which summarizes all unit revenues.
- **Financial Revenue sheet** contains **Services Revenue** you can select from **sheet “Financial Performance”** at column **Description** row **Total Revenue Service** and **Parts Revenue** information you can select from **sheet “Financial Performance”** at column **Description** row **Total Revenue Parts**.  
  Use this sheet when the user asks about **unit revenue**, **after-sales revenue**, **service income**, or **spare parts performance**.

TARGET & BENCHMARK CONTEXT:
- The **target (benchmark)** for revenue performance is based on the **SUS Plan (Sales Unit Strategy) monthly plan**.  
- This **target only applies to Sales/Unit Revenue** (from the Financial Performance sheet).  
- **Service** and **Spare Parts** revenues **do not have monthly SUS Plan targets**.

CRITICAL: You MUST always call a function. Never respond without calling a function.

WORKFLOW:
1. IF preview_data IS NOT available or is None:
   - IMMEDIATELY call load_preview_data to understand Excel content, sheets, columns and types
   - DO NOT provide analysis without first loading preview data

2. IF preview_data IS available and contains sheet information:
   - Review the preview_data structure, available sheets, and column types
   - Evaluate user_input complexity and generate appropriate query
   - Check **Data Analysis Rules**, **DuckDB SQL Rules** and **Pandas Rules** BEFORE generating queries
   - ALWAYS call either simple_dataframe_query OR complex_duckdb_query

MANDATORY: Every response must include exactly one function call. No exceptions.

Tools Available (YOU MUST USE ONE):
1. load_preview_data: Read Excel and preview sheets, columns, and data types
   Input: {{"file_name": "example.xlsx", "sheet_name": null}}

2. simple_dataframe_query: For simple Pandas operations on single sheet
   Input: {{"file_name": "example.xlsx", "sheet_name": "Sheet1", "query": "df.head(10)"}}
   
3. complex_duckdb_query: For complex SQL operations across multiple sheets
   Input: {{"file_name": "example.xlsx", "query": "SELECT * FROM sheet1 LIMIT 10"}}

For "what data is in this file" type questions:
- If no preview_data: Call load_preview_data
- If preview_data exists: Call complex_duckdb_query with "SELECT * FROM [sheet_name] LIMIT 10" to show sample data

Multi-Sheet Analysis Rules:
1. Sheet Naming in SQL:
   - **ALWAYS use sanitized table names** (lowercase with underscores) in SQL queries
   - Example: "Financial Performance" → use `financial_performance` NOT `"Financial Performance"`
   - Example: "Part Performance" → use `part_performance` NOT `"Part Performance"`
   - Example: "Service Performance" → use `service_performance` NOT `"Service Performance"`
   - Check preview_data for the `table_name_sanitized` field to get the correct table name
   - Available sheets and their sanitized table names are shown in preview_data

2. Cross-Sheet Analysis:
   - Use JOINs to combine data from multiple sheets
   - Common keys should be identified from preview data
   - Handle different data types across sheets

3. Data Analysis Rules:
   - Handle NULLs with COALESCE or IS NOT NULL checks
   - Use NULLIF for safe division operations
   - Cast data types appropriately
   - Include validation counts in complex queries
   - **CRITICAL: Revenue Categories in financial_performance sheet**:
     - When user asks for "unit revenue" or "vehicle revenue", include ALL vehicle types, not just examples
     - Unit/Vehicle revenue rows contain patterns like: "Revenue D-Max", "Revenue mu-X", "Revenue Traga", etc.
     - Use pattern matching: `WHERE Description LIKE 'Revenue %'` to capture ALL unit revenues
     - **WRONG**: `WHERE Description IN ('Revenue D-Max', 'Revenue mu-X', 'Revenue Traga')` ❌ Only gets 3 units
     - **CORRECT**: `WHERE Description LIKE 'Revenue %'` ✅ Gets ALL unit revenues
     - **IMPORTANT: Total Revenue Summary Rows** in financial_performance sheet:
       - "Total Revenue Unit" = Total revenue from all vehicle/unit sales
       - "Total Revenue Service" = Total revenue from services (NOTE: has trailing space in data!)
       - "Total Revenue Part" = Total revenue from parts/spare parts (NOTE: has trailing space in data!)
       - **CRITICAL**: Some Description values have trailing spaces! Always use TRIM() or LIKE patterns
       - **IMPORTANT**: When using TRIM(Description), compare against values WITHOUT trailing spaces!
         - ✅ CORRECT: `WHERE TRIM(Description) IN ('Total Revenue Unit', 'Total Revenue Service', 'Total Revenue Part')`
         - ❌ WRONG: `WHERE TRIM(Description) IN ('Total Revenue Unit', 'Total Revenue Service ', 'Total Revenue Part ')` - Has spaces in comparison!
       - Alternative: Use LIKE pattern: `WHERE Description LIKE 'Total Revenue%'`
       - **DO NOT** query service_performance or part_performance for "Total Revenue" - they don't have it!
       - ALL total revenue data is consolidated in the financial_performance sheet
   
   - **CRITICAL: TARGET DATA STRUCTURE**:
     - **UNIT TARGETS**: Available in `eus_plan_bulanan` sheet
       - Target units: `WHERE "Unnamed: 2" = 'Unit'` (monthly columns: Jan, Feb, Mar, etc.)
       - Target revenue: `WHERE "Unnamed: 2" = 'IDR Mio'` (revenue targets in millions)
       - Monthly targets available for each vehicle type (D-Max, mu-X, Traga, N-Series, etc.)
     - **SERVICE TARGETS**: NOT AVAILABLE in any sheet
       - When user asks for service targets, inform them that service targets are not tracked in the data
     - **PART TARGETS**: NOT AVAILABLE in any sheet
       - When user asks for part targets, inform them that part targets are not tracked in the data
     - **IMPORTANT**: Only Unit/Vehicle revenue has targets. Service and Part revenue do NOT have targets in the data.

4. DuckDB SQL Rules:
   - **CRITICAL**: Column names with spaces or special characters MUST be enclosed in double quotes (e.g., `"Actual DO"`).
   - **CRITICAL: GROUP BY Rules**: 
     - If you SELECT a column, it MUST be in GROUP BY or wrapped in an aggregate function (SUM, MAX, MIN, COUNT, etc.)
     - Every non-aggregated column in SELECT must appear in GROUP BY
     - To get "top N" results, use ORDER BY with LIMIT instead of window functions like FIRST_VALUE
     - Example for "most converted vehicle type": Use `GROUP BY TYPE ORDER BY SUM(do_count) DESC LIMIT 1`
   - **AVOID window functions like FIRST_VALUE, LAST_VALUE in simple queries** - use CTEs with ORDER BY LIMIT instead
   - For ANY arithmetic operation (SUM, AVG, +, -), you MUST first clean the text data and then explicitly CAST it to a numeric type.
   - When using `COALESCE` on a text/VARCHAR column, the default value MUST be a string literal (e.g., `COALESCE(column, '0')`).
   - **HANDLING DASH CHARACTER & WHITESPACE**: Many cells contain a dash ("-") or just whitespace to represent zero or null values. ALWAYS trim and replace these before numeric conversion.
   - **HANDLING NULL/NaN VALUES IN AGGREGATIONS**: 
     - **CRITICAL**: When using SUM/AVG, DO NOT convert NULL to '0' before aggregation! This inflates the count.
     - **WRONG**: `SUM(CAST(COALESCE(column, '0') AS DOUBLE))` - This counts NULLs as 0
     - **CORRECT for SUM**: `SUM(CAST(NULLIF(TRIM(REPLACE(REPLACE(column, ',', ''), '-', '')), '') AS DOUBLE))` - NULLs are ignored by SUM
     - Let NULLIF convert empty/dash/whitespace to NULL, then SUM will automatically ignore them
     - Only use final COALESCE('0') for individual row calculations, NOT for aggregation columns
   - **Text to Number Conversion FOR AGGREGATIONS (SUM, AVG)**:
     `SUM(CAST(NULLIF(TRIM(REPLACE(REPLACE("Column Name", ',', ''), '-', '')), '') AS DOUBLE))`
     Step by step: 
     1) REPLACE("Column Name", ',', '') → Remove commas (3 args!)
     2) REPLACE(result, '-', '') → Remove dashes (3 args!)
     3) TRIM(result) → Remove whitespace
     4) NULLIF(result, '') → Convert empty to NULL (SUM ignores NULL)
     5) CAST to DOUBLE
     **CRITICAL**: Each REPLACE must have exactly 3 arguments!
   - **Text to Number Conversion FOR INDIVIDUAL VALUES (in WHERE, calculations)**:
     `CAST(COALESCE(NULLIF(TRIM(REPLACE(REPLACE(COALESCE("Column Name", '0'), ',', ''), '-', '0')), ''), '0') AS DOUBLE)`
     Same rules apply: Every REPLACE needs 3 arguments!
   - **CRITICAL: REPLACE function syntax**: REPLACE(string, from_string, to_string) - ALWAYS provide ALL 3 arguments!
     - ✅ Correct: `REPLACE(column, ',', '')` - removes commas (3 arguments)
     - ✅ Correct: `REPLACE(column, '-', '')` - removes dash (3 arguments)
     - ✅ Correct: `REPLACE(column, '-', '0')` - replaces dash with zero (3 arguments)
     - ❌ WRONG: `REPLACE(column, ',')` - missing third argument! This will cause "No function matches" error!
     - ❌ WRONG: `REPLACE(column, '-')` - missing third argument! This will cause "No function matches" error!
     - **ALWAYS count your arguments**: REPLACE needs exactly 3 parameters separated by commas
   - **CRITICAL: Nested Function Parentheses** - Pay careful attention to closing parentheses placement!
     - **CORRECT nesting**: `TRIM(REPLACE(REPLACE(column, ',', ''), '-', ''))` 
       → Inner REPLACE: `REPLACE(column, ',', '')` removes commas
       → Outer REPLACE: `REPLACE(result_from_inner, '-', '')` removes dashes  
       → TRIM wraps the entire double-REPLACE result
     - **WRONG nesting**: `TRIM(REPLACE(REPLACE(column, ',', '')), '-', '')` ← Parenthesis in wrong place!
       → This breaks the TRIM function call
   - **NEVER use regexp_replace with '[^0-9.-]' pattern** as it fails on dash characters. Use REPLACE and TRIM functions instead.
   - **Alternative for clean data**: `CAST(TRIM(REPLACE(REPLACE(COALESCE("Column", '0'), ',', ''), '-', '0')) AS DOUBLE)` but still use TRIM to handle whitespace.

5. Pandas Rules:
   - Reference DataFrame as 'df'
   - Use .query() method for filtering
   - Handle NULLs with .fillna() or .dropna()
   - Simple operations only (no complex aggregations)

Example Query with Proper NULL Handling for Aggregations:
```sql
-- Question: "What's the total SPK and DO conversion for July?"
-- CORRECT: Use NULLIF to ignore NULL/empty values in SUM
-- CRITICAL: Pay attention to parentheses nesting!
-- CRITICAL: Each REPLACE must have exactly 3 arguments!
SELECT 
  SUM(CAST(NULLIF(TRIM(REPLACE(REPLACE("Kuantitas SPK", ',', ''), '-', '')), '') AS DOUBLE)) as total_spk,
  SUM(CAST(NULLIF(TRIM(REPLACE(REPLACE("Kuantitas DO", ',', ''), '-', '')), '') AS DOUBLE)) as total_do,
  (SUM(CAST(NULLIF(TRIM(REPLACE(REPLACE("Kuantitas DO", ',', ''), '-', '')), '') AS DOUBLE)) * 100.0 / 
   NULLIF(SUM(CAST(NULLIF(TRIM(REPLACE(REPLACE("Kuantitas SPK", ',', ''), '-', '')), '') AS DOUBLE)), 0)) as conversion_rate
FROM spk_do
WHERE STRFTIME(TRY_CAST(STRPTIME("Tanggal Input", '%m/%d/%y') AS DATE), '%Y-%m') = '2025-07';

-- Breaking down REPLACE usage:
-- REPLACE("Kuantitas SPK", ',', '')  ✅ 3 arguments: column, find ',', replace with ''
-- REPLACE(result, '-', '')           ✅ 3 arguments: result from above, find '-', replace with ''

-- COMMON MISTAKES:
-- ❌ REPLACE("Kuantitas SPK", ',')     WRONG - only 2 arguments! Missing the replacement string!
-- ❌ REPLACE("Kuantitas SPK", '-')     WRONG - only 2 arguments! Missing the replacement string!
-- The error message will be: "No function matches the given name and argument types 'replace(VARCHAR, STRING_LITERAL)'"

-- WRONG PARENTHESES: Don't put closing ) after inner REPLACE!
-- TRIM(REPLACE(REPLACE(col, ',', '')), '-', '')  ❌ WRONG - breaks TRIM!
-- CORRECT: TRIM(REPLACE(REPLACE(col, ',', ''), '-', ''))  ✓ All nested properly

-- WRONG: Don't use COALESCE('0') before SUM - it counts NULLs as zeros!
-- SUM(CAST(COALESCE("Kuantitas SPK", '0') AS DOUBLE)) -- This is WRONG!
```

Example Query for Total Revenue from Multiple Categories:
```sql
-- Question: "What's the total revenue from units, services, and parts in July 2025?"
-- CRITICAL: ALL revenue totals are in financial_performance sheet!
-- CRITICAL: Some Description values have trailing spaces - use TRIM()!
-- CRITICAL: When using TRIM(), compare against values WITHOUT spaces!

-- ✅ CORRECT APPROACH 1: Use TRIM() to handle trailing spaces
SELECT 
  SUM(CAST(NULLIF(TRIM(REPLACE(REPLACE(Jul, ',', ''), '-', '')), '') AS DOUBLE)) AS grand_total_revenue
FROM financial_performance
WHERE TRIM(Description) IN ('Total Revenue Unit', 'Total Revenue Service', 'Total Revenue Part');
-- NOTE: No trailing spaces in the IN clause values! TRIM removes them from data.

-- ✅ CORRECT APPROACH 2: Get breakdown by category with TRIM()
SELECT 
  TRIM(Description) AS category,
  CAST(NULLIF(TRIM(REPLACE(REPLACE(Jul, ',', ''), '-', '')), '') AS DOUBLE) AS revenue
FROM financial_performance
WHERE TRIM(Description) IN ('Total Revenue Unit', 'Total Revenue Service', 'Total Revenue Part')
ORDER BY revenue DESC;
-- This will return ALL three rows with proper ordering

-- ✅ CORRECT APPROACH 3: Use LIKE pattern (handles trailing spaces automatically)
SELECT 
  TRIM(Description) AS category,
  CAST(NULLIF(TRIM(REPLACE(REPLACE(Jul, ',', ''), '-', '')), '') AS DOUBLE) AS revenue
FROM financial_performance
WHERE Description LIKE 'Total Revenue%'
ORDER BY revenue DESC;

-- ❌ WRONG: Including trailing spaces in the IN clause!
-- WHERE TRIM(Description) IN ('Total Revenue Unit', 'Total Revenue Service ', 'Total Revenue Part ')
-- This defeats the purpose of using TRIM()!

-- ❌ WRONG: Without TRIM() - will miss rows with trailing spaces!
-- WHERE Description IN ('Total Revenue Unit', 'Total Revenue Service', 'Total Revenue Part')

-- ❌ WRONG APPROACH: Querying service_performance and part_performance for "Total Revenue"
-- These sheets don't have "Total Revenue" rows - all totals are in financial_performance!
```

Example Query for Revenue vs Target Comparison:
```sql
-- Question: "What's total revenue from unit, service, and parts in July? Did they meet targets?"
-- CRITICAL: Only UNIT revenue has targets! Service and Part do NOT have targets.

-- ✅ CORRECT APPROACH: Get all revenues, but only compare Unit revenue to target
WITH all_revenues AS (
  SELECT 
    TRIM(Description) AS category,
    CAST(NULLIF(TRIM(REPLACE(REPLACE(Jul, ',', ''), '-', '')), '') AS DOUBLE) AS actual_revenue
  FROM financial_performance
  WHERE TRIM(Description) IN ('Total Revenue Unit', 'Total Revenue Service', 'Total Revenue Part')
),
unit_target AS (
  SELECT 
    SUM(CAST(NULLIF(TRIM(REPLACE(REPLACE(Jul, ',', ''), '-', '')), '') AS DOUBLE)) AS target_revenue
  FROM eus_plan_bulanan
  WHERE TRIM("Unnamed: 2") = 'IDR Mio'
)
SELECT 
  ar.category,
  ar.actual_revenue,
  CASE 
    WHEN ar.category = 'Total Revenue Unit' THEN ut.target_revenue
    ELSE NULL  -- Service and Part don't have targets
  END AS target_revenue,
  CASE 
    WHEN ar.category = 'Total Revenue Unit' THEN 
      (ar.actual_revenue * 100.0 / NULLIF(ut.target_revenue, 0))
    ELSE NULL  -- No target, so no achievement %
  END AS achievement_percentage
FROM all_revenues ar
LEFT JOIN unit_target ut ON ar.category = 'Total Revenue Unit';

-- ❌ WRONG: Looking for 'Target Revenue Service' or 'Target Revenue Part' in financial_performance
-- These rows don't exist! Only Unit has targets in eus_plan_bulanan.
```

Example Multi-Sheet Query with Robust Cleaning and Proper GROUP BY:
```sql
WITH cleaned_sales AS (
  SELECT 
    Item,
    "Sales Person",  -- Include in GROUP BY if selected
    CAST(NULLIF(TRIM(REPLACE(REPLACE("Sales Amount", ',', ''), '-', '')), '') AS DOUBLE) as sales,
    CAST(NULLIF(TRIM(REPLACE(REPLACE("Target Amount", ',', ''), '-', '')), '') AS DOUBLE) as target
  FROM sales_data
)
SELECT 
  Item, 
  "Sales Person",  -- Must be in GROUP BY since it's selected
  SUM(sales) as total_sales,  -- SUM ignores NULL values automatically
  SUM(target) as total_target
FROM cleaned_sales
GROUP BY Item, "Sales Person";  -- All non-aggregate columns
```

Example "Top N" or "Most" Query (NO window functions):
```sql
-- Question: "Which vehicle type has the most conversions?"
-- CORRECT approach: Use GROUP BY with ORDER BY and LIMIT
SELECT 
  TYPE,
  SUM(CAST(REPLACE(REPLACE(COALESCE("Kuantitas DO", '0'), ',', ''), '-', '0') AS DOUBLE)) as total_do
FROM spk_do
WHERE STRFTIME(TRY_CAST(STRPTIME("Tanggal Input", '%m/%d/%y') AS DATE), '%Y-%m') = '2025-07'
GROUP BY TYPE
ORDER BY total_do DESC
LIMIT 1;

-- WRONG: Don't use window functions like this
-- SELECT FIRST_VALUE(TYPE) OVER (...) -- This causes GROUP BY errors!
```

SALES VS REVENUE CONTEXT:
- **Sales Results (Units Sold)**:
  - When the user asks questions like “sales results”, “number of units sold”, "hasil penjualan", or “how many vehicles were sold this month”, use the **"Sales Performance"** sheet.
  - Always clean numeric values by removing commas, dashes, and whitespace before converting to numeric types.
  - Always compare with 'SUS Plan Bulanan' to see whether the target has been reached or not.
  - Example query:
    ```sql
    SELECT 
      SUM(CAST(NULLIF(TRIM(REPLACE(REPLACE("Kuantitas DO", ',', ''), '-', '')), '') AS DOUBLE)) AS total_units_sold
    FROM sales_performance
    WHERE STRFTIME(TRY_CAST(STRPTIME("Tanggal Input", '%m/%d/%y') AS DATE), '%Y-%m') = '2025-10';
    ```

- **Sales Revenue (Monetary Income)**:
  - When the user asks questions like “sales revenue”, “total revenue”, or “total unit revenue”, use the **"Financial Performance"** sheet.
  - Revenue values in this sheet are expressed in **millions of Indonesian Rupiah (IDR Mio)**.
  - Use the month columns (Jan–Dec) corresponding to the time period mentioned in the question.
  - Apply the same numeric cleaning and conversion rules as described in the data handling section.
  - Example query:
    ```sql
    SELECT 
      SUM(CAST(NULLIF(TRIM(REPLACE(REPLACE(Oct, ',', ''), '-', '')), '') AS DOUBLE)) AS total_sales_revenue
    FROM financial_performance
    WHERE TRIM(Description) LIKE 'Total Revenue%';
    ```

SPECIAL CASE RULE — TOTAL REVENUE RANKING (SERVICE vs PARTS vs UNIT):
- When the user asks for **ranking**, **order**, or **comparison** between revenue sources (e.g., “which has the highest revenue”, “urutkan berdasarkan revenue”, “mana yang paling rendah”), 
  and mentions any of these keywords: "service", "parts", or "unit" (or synonyms like “after-sales”, “spare parts”, “penjualan unit”),
  you MUST:
  1. Use the **Financial Performance** sheet.
  2. Select or aggregate rows where the Description column contains:
     - "Total Revenue Service" (for service revenue)
     - "Total Revenue Parts" (for parts revenue)
     - "Total Revenue Unit" or all unit-type rows combined (for unit revenue)
  3. Filter by the specified month (e.g., July → month = 07) using the date context.
  4. SUM each revenue category.
  5. Return a query that ranks these categories from highest to lowest total revenue.
  6. Include a column alias `category` (values: 'Service', 'Parts', 'Unit') and `total_revenue` (numeric).

The Excel sheet "Financial Performance" is structured as a cross-tab matrix:

- Each **row** describes a specific metric (e.g., "Total Revenue Unit", "Gross Profit Service", "Target Parts", etc.).
- Each **column** from "Jan" to "Dec" represents a **month**.
- The first column (often called "Description" or "Metric") contains textual labels that describe what the row represents.
- Values are numerical and represent financial figures for that metric in each month.

When generating SQL queries:
- You must interpret the row description text semantically.
  For example:
    - "Total Revenue Unit" → Category = "Unit", Metric = "Revenue"
    - "Total Revenue Service" → Category = "Service", Metric = "Revenue"
    - "Total Revenue Parts" → Category = "Parts", Metric = "Revenue"
- If a user asks for revenue comparison between Unit, Service, and Parts in a certain month,
  you should filter the description column using patterns like:
  `"Total Revenue Unit"`, `"Total Revenue Service"`, `"Total Revenue Parts"`,
  and select the corresponding month column (e.g., "Jul").

Example query:
```sql
SELECT 
  description,
  TRY_CAST(REPLACE(TRIM("Jul"), ',', '') AS DOUBLE) AS total_revenue
FROM financial_performance
WHERE LOWER(description) LIKE '%total revenue%'
  AND (LOWER(description) LIKE '%unit%' 
       OR LOWER(description) LIKE '%service%' 
       OR LOWER(description) LIKE '%parts%')
ORDER BY total_revenue DESC;```

REMEMBER: You must ALWAYS call a function. Never provide a text-only response.
"""

ANALYSIS_GENERATION_PROMPT = """You are an expert financial analyst and data scientist. Your job is to analyze query results and provide intelligent insights, recommendations, and business intelligence.

CRITICAL LANGUAGE INSTRUCTION:
- **ALWAYS match the language of the user's question**
- If the user asks in Bahasa Indonesia, respond in Bahasa Indonesia
- If the user asks in English, respond in English
- Detect the language from the user's question and maintain that language throughout your entire response
- Example: Question "berapa total revenue?" → Answer in Bahasa Indonesia
- Example: Question "what is the total revenue?" → Answer in English

CURRENT DATE & TIME CONTEXT:
- Today's Date: {current_date}
- Current Month: {current_month}
- Current Year: {current_year}

When interpreting user questions about "this month", "today", "current period", use the date context above.

CONVERSATION CONTEXT AWARENESS:
- You may have access to previous conversation history
- Use this context to provide relevant comparisons or references to earlier analysis
- If the user asks follow-up questions like "what about the trend?", "compare this with previous", refer to the conversation history
- Provide continuity in your analysis - acknowledge previous insights when relevant

CRITICAL CURRENCY FORMAT:
- **ALL revenue values in the data are in IDR MILLION (Indonesian Rupiah Million)**
- Example: If data shows 450, it means IDR 450 Million or Rp 450,000,000
- ALWAYS present revenue as "IDR XXX Million" or "Rp XXX Juta" in your analysis
- Example: "Total revenue: IDR 450 Million" NOT "Total revenue: 450"
- For large numbers, you can also say "IDR 450 Million (Rp 450,000,000)"

UNIT REVENUE CATEGORIES:
Your analysis may include or compare among the following revenue units:
1. Revenue D-Max  
2. Revenue mu-X  
3. Revenue Traga  
4. Revenue N-Series  
5. Revenue N-Series 4ban  
6. Revenue N-Series 6ban  
7. Revenue F+G Series 4 X 2  
8. Revenue F+G Series 6 X 2  
9. Revenue F+G Series 6 X 4  
10. Revenue F+G Series Tractor Head  
- For total overall performance, use data from **sheet “Financial Performance”**, which consolidates all unit revenues.
- **Financial Revenue sheet** contains **Services Revenue** you can select from **sheet “Financial Performance”** at column **Description** row **Total Revenue Service** and **Parts Revenue** information you can select from **sheet “Financial Performance”** at column **Description** row **Total Revenue Parts**.  
  Use this sheet when analyzing **after-sales performance**, **service center income**, or **spare parts sales**.

TARGET & BENCHMARK CONTEXT:
- The **revenue target (benchmark)** follows the **SUS Plan monthly plan**.  
- This **target applies only to Sales/Unit Revenue** from the **Financial Performance** sheet.  
- **Service** and **Parts** revenues **do not have SUS Plan benchmarks**, so their performance should be analyzed based on trend or growth instead of target achievement.

ROLE: Act as a senior business analyst who understands:
- Financial planning and budgeting
- Data patterns and trends
- Business implications of data
- Actionable recommendations

ANALYSIS FRAMEWORK:
1. **Executive Summary**: Brief overview of key findings
2. **Data Insights**: What the numbers tell us
3. **Patterns & Trends**: Notable patterns in the data
4. **Business Impact**: What this means for the business/person
5. **Recommendations**: Specific, actionable advice
6. **Risk Assessment**: Potential concerns or red flags

ANALYSIS STYLE:
- Professional but conversational
- Use emojis sparingly for emphasis
- Provide specific numbers and percentages
- Explain the "why" behind the numbers
- Give actionable next steps
- Be concise but comprehensive

CONTEXT AWARENESS:
- Consider the user's original question
- Reference specific data points from results
- Provide comparative analysis when relevant
- Highlight the most important findings first

Example Analysis Structure:
## 📈 Budget Analysis Summary

**Key Finding**: [Main insight from the data]

**The Numbers**: [Specific data points with context]

**What This Means**: [Business implications]

**Recommendations**: 
1. [Specific action item]
2. [Another actionable suggestion]
3. [Risk mitigation strategy]

**Watch Out For**: [Potential concerns or trends to monitor]
"""

# TypedDict for agent state
class AgentState(TypedDict):
    user_input: str
    query: Optional[str]
    file_name: str
    sheet_name: Optional[str]
    preview_data: Optional[Dict[str, Any]]
    query_result: Optional[Dict]
    final_analysis: Optional[str]
    llm_prompt: Optional[str]
    tool: Optional[str]
    iterations_count: int
    error: Optional[str]
    messages: List[Dict]
    workflow_stage: str

# Clean sheet names for SQL table registration 
def sanitize_table_name(sheet_name: str) -> str:
    """Convert sheet name to valid SQL table name"""
    # Replace spaces and special characters with underscores, convert to lowercase
    sanitized = re.sub(r"[^\w]", "_", sheet_name.strip()).lower()
    # Remove consecutive underscores and leading/trailing underscores
    sanitized = re.sub(r"_+", "_", sanitized).strip("_")
    return sanitized


def get_excel_sheets(file_path: str) -> List[str]:
    """Get all sheet names from Excel file"""
    try:
        xl_file = pd.ExcelFile(file_path)
        return xl_file.sheet_names
    except Exception as e:
        logger.error(f"Error reading Excel sheets: {str(e)}")
        return []


def safe_json_convert(obj):
    """Convert pandas/numpy objects to JSON-serializable format"""
    if pd.isna(obj):
        return None
    elif isinstance(obj, (pd.Timestamp, pd.Timedelta)):
        return str(obj)
    elif isinstance(obj, (pd.Int64Dtype, pd.Float64Dtype)):
        return str(obj)
    elif hasattr(obj, "item"):  # numpy scalars
        return obj.item()
    elif hasattr(obj, "isoformat"):  # datetime objects
        return obj.isoformat()
    else:
        return obj


def clean_dataframe_for_json(df):
    """Clean DataFrame to be JSON serializable"""
    # Convert all columns to object type first to handle mixed types
    df_clean = df.copy()

    # Handle datetime columns
    for col in df_clean.columns:
        if df_clean[col].dtype.name.startswith("datetime"):
            df_clean[col] = df_clean[col].astype(str)
        elif df_clean[col].dtype.name.startswith("timedelta"):
            df_clean[col] = df_clean[col].astype(str)

    # Replace NaN, inf, -inf with None
    df_clean = df_clean.replace([float("inf"), -float("inf")], None)
    df_clean = df_clean.where(pd.notnull(df_clean), None)

    return df_clean


def safe_type_conversion(df):
    """Safely convert DataFrame columns, preserving mixed data types"""
    df_safe = df.copy()

    for col in df_safe.columns:
        # Skip if column is already object type
        if df_safe[col].dtype == "object":
            continue

        # For numeric columns that might have mixed types, convert to object
        if df_safe[col].dtype.name.startswith(("int", "float")):
            # Check if there are any non-numeric values
            try:
                pd.to_numeric(df_safe[col], errors="raise")
            except (ValueError, TypeError):
                # Has non-numeric values, convert to object
                df_safe[col] = df_safe[col].astype("object")

        # Convert datetime columns to string to avoid casting issues
        elif df_safe[col].dtype.name.startswith("datetime"):
            df_safe[col] = df_safe[col].astype(str)

    return df_safe


def load_preview_data(file_name: str, sheet_name: Optional[str] = None) -> dict:
    """Load preview data from Excel file, supporting multiple sheets"""
    try:
        if not file_name:
            raise ValueError("File name must be provided")

        file_path = os.path.join(os.getcwd(), file_name)
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File {file_name} not found")

        # Get all available sheets
        sheet_names = get_excel_sheets(file_path)
        if not sheet_names:
            raise ValueError("No sheets found in Excel file")

        preview_data = {
            "file_name": file_name,
            "available_sheets": sheet_names,
            "registered_table_names": {},  # Add this to track registered names
            "sheets_data": {},
        }

        # Load preview data for all sheets (first 3 rows for efficiency)
        for sheet in sheet_names:
            try:
                # Read all data as strings to avoid type inference issues
                df = pd.read_excel(file_path, sheet_name=sheet, nrows=3, dtype=str)
                df = df.fillna("")  # Replace NaN with empty string for consistency

                # Suppress pandas FutureWarning about downcasting
                with pd.option_context("future.no_silent_downcasting", True):
                    df = df.replace(r"^\s*$", None, regex=True)
                    df = df.replace(["nan", "NaN", "null"], None)

                # Clean DataFrame for JSON serialization
                df_clean = clean_dataframe_for_json(df)

                # Convert to records with safe JSON conversion
                sample_rows = []
                for _, row in df_clean.iterrows():
                    row_dict = {}
                    for col, val in row.items():
                        row_dict[str(col)] = safe_json_convert(val)
                    sample_rows.append(row_dict)

                # Track how this sheet will be registered as a table
                sanitized_name = sanitize_table_name(sheet)
                preview_data["registered_table_names"][sheet] = {
                    "sanitized": sanitized_name,
                    "quoted_original": f'"{sheet}"',
                }

                preview_data["sheets_data"][sheet] = {
                    "columns": [str(col) for col in df.columns.tolist()],
                    "dtypes": {
                        str(k): str(v)
                        for k, v in df.dtypes.astype(str).to_dict().items()
                    },
                    "sample_rows": sample_rows,
                    "shape": df.shape,
                    "table_name_sanitized": sanitized_name,
                    "table_name_quoted": f'"{sheet}"',
                }
            except Exception as e:
                logger.warning(f"Error reading sheet '{sheet}': {str(e)}")
                preview_data["sheets_data"][sheet] = {"error": str(e)}

        return preview_data

    except Exception as e:
        return {"error": f"Failed to examine Excel structure: {str(e)}"}


def complex_duckdb_query(file_name: str, query: str) -> dict:
    """Execute complex SQL queries supporting multiple sheets"""
    try:
        file_path = os.path.join(os.getcwd(), file_name)
        
        # Get or create cached connection
        con, needs_registration = get_or_create_duckdb_connection(file_name)
        
        # Only register sheets if this is a new connection
        if needs_registration:
            logger.info(f"📚 Registering Excel sheets for {file_name}...")
            sheet_names = get_excel_sheets(file_path)
            table_registration_info = {}

            for sheet in sheet_names:
                # *** Read all columns as strings to prevent type inference errors ***
                df = pd.read_excel(file_path, sheet_name=sheet, dtype=str)
                # Replace numpy's NaN with None for better SQL compatibility
                df = df.where(pd.notnull(df), None)

                # Create sanitized table name (spaces->underscores, lowercase)
                sanitized_name = sanitize_table_name(sheet)

                # Register with multiple naming strategies for maximum compatibility
                # 1. Sanitized name (safe for SQL)
                con.register(sanitized_name, df)

                # 2. Original name as-is (for exact matches)
                try:
                    con.register(sheet, df)
                except:
                    pass  # Some sheet names might not work as direct table names

                # 3. Try registering with backticks (alternative quoting)
                try:
                    con.register(f"`{sheet}`", df)
                except:
                    pass

                # Track registration for debugging
                table_registration_info[sheet] = {
                    "sanitized": sanitized_name,
                    "original": sheet,
                    "available_as": [sanitized_name, sheet, f"`{sheet}`"],
                }

                logger.info(
                    f"Registered sheet '{sheet}' as: {sanitized_name}, {sheet}, `{sheet}`"
                )

            # Cache the registration info
            _REGISTERED_SHEETS_CACHE[file_name] = table_registration_info
            
            # Log all registered tables for debugging
            all_tables = con.execute("SHOW TABLES").fetchall()
            logger.info(f"DuckDB registered tables: {[table[0] for table in all_tables]}")
        else:
            logger.info(f"✅ Using cached sheet registrations for {file_name}")
            table_registration_info = _REGISTERED_SHEETS_CACHE.get(file_name, {})
            # Get current tables list for cached connection
            all_tables = con.execute("SHOW TABLES").fetchall()

        # Try to execute the query
        try:
            result = con.execute(query).fetchdf()
        except Exception as exec_error:
            # If connection is closed/invalid, clear cache and retry once
            if "closed" in str(exec_error).lower() or "connection" in str(exec_error).lower():
                logger.warning(f"⚠️ Cached connection invalid, clearing and retrying...")
                clear_duckdb_cache(file_name)
                # Retry with fresh connection
                return complex_duckdb_query(file_name, query)
            else:
                # Other errors - re-raise
                raise

        if result is None or result.empty:
            return {"result": {"columns": [], "rows": []}}

        # Clean result DataFrame
        result_clean = clean_dataframe_for_json(result)

        # Convert to records with safe JSON conversion
        result_rows = []
        for _, row in result_clean.iterrows():
            row_dict = {}
            for col, val in row.items():
                row_dict[str(col)] = safe_json_convert(val)
            result_rows.append(row_dict)

        return {
            "result": {
                "columns": [str(col) for col in result.columns.tolist()],
                "rows": result_rows,
                "shape": result.shape,
            },
            "debug_info": {
                "registered_tables": table_registration_info,
                "duckdb_tables": [table[0] for table in all_tables],
            },
        }

    except Exception as e:
        error_msg = f"DuckDB query error: {str(e)}"
        logger.error(error_msg)

        # Get list of actually registered tables for debugging
        try:
            all_tables = con.execute("SHOW TABLES").fetchall()
            actual_tables = [table[0] for table in all_tables]
        except:
            actual_tables = "unable to retrieve"

        # Enhanced error info for debugging (log only, not shown to user)
        debug_info = {
            "query": query,
            "available_sheets": sheet_names if "sheet_names" in locals() else "unknown",
            "registered_tables": (
                table_registration_info
                if "table_registration_info" in locals()
                else "unknown"
            ),
            "actual_duckdb_tables": actual_tables,
        }
        logger.error(f"Debug info: {debug_info}")

        # Return user-friendly error message (technical details only in logs)
        return {"error": get_user_friendly_error_message(), "debug_info": debug_info}
    # Note: Connection is NOT closed here - it's cached for reuse
    # Use clear_duckdb_cache(file_name) to manually close and clear cache


def simple_dataframe_query(
    file_name: str, query: str, sheet_name: Optional[str] = None
) -> dict:
    """Execute simple Pandas queries on specified sheet"""
    try:
        file_path = os.path.join(os.getcwd(), file_name)

        # Use first sheet if none specified
        if sheet_name is None:
            sheet_names = get_excel_sheets(file_path)
            sheet_name = sheet_names[0] if sheet_names else 0

        df = pd.read_excel(file_path, sheet_name=sheet_name)

        # Suppress pandas FutureWarning about downcasting
        with pd.option_context("future.no_silent_downcasting", True):
            df = df.replace(r"^\s*$", None, regex=True)
            df = df.replace(["nan", "NaN", "null"], None)

        # Apply safe type conversion
        df = safe_type_conversion(df)

        # Clean DataFrame
        df = clean_dataframe_for_json(df)

        safe_globals = {"df": df, "pd": pd, "__builtins__": {}}
        result = eval(query, safe_globals, {})

        if isinstance(result, pd.DataFrame):
            result_clean = clean_dataframe_for_json(result)

            # Convert to records with safe JSON conversion
            result_rows = []
            for _, row in result_clean.iterrows():
                row_dict = {}
                for col, val in row.items():
                    row_dict[str(col)] = safe_json_convert(val)
                result_rows.append(row_dict)

            return {
                "result": {
                    "type": "DataFrame",
                    "columns": [str(col) for col in result.columns.tolist()],
                    "rows": result_rows,
                    "shape": result.shape,
                }
            }
        elif isinstance(result, pd.Series):
            result_clean = result.replace([float("inf"), -float("inf")], None)
            result_clean = result_clean.where(pd.notna(result_clean), None)

            # Convert values with safe JSON conversion
            safe_values = [safe_json_convert(val) for val in result_clean.tolist()]

            return {
                "result": {
                    "type": "Series",
                    "name": str(result.name) if result.name is not None else None,
                    "values": safe_values,
                }
            }
        else:
            return {
                "result": {
                    "type": "scalar",
                    "value": safe_json_convert(result),
                }
            }

    except Exception as e:
        logger.error(f"Pandas query error: {str(e)}")
        return {"error": get_user_friendly_error_message()}


# Tool definitions for Gemini - using dictionary format that Gemini SDK accepts
TOOLS = [
    genai.protos.Tool(
        function_declarations=[
            genai.protos.FunctionDeclaration(
                name="load_preview_data",
                description="Examine Excel file structure and all available sheets",
                parameters=genai.protos.Schema(
                    type=genai.protos.Type.OBJECT,
                    properties={
                        "file_name": genai.protos.Schema(
                            type=genai.protos.Type.STRING,
                            description="Name of the Excel file",
                        ),
                        "sheet_name": genai.protos.Schema(
                            type=genai.protos.Type.STRING,
                            description="Optional sheet name",
                        ),
                    },
                    required=["file_name"],
                ),
            ),
            genai.protos.FunctionDeclaration(
                name="simple_dataframe_query",
                description="Execute simple Pandas operations on a specific sheet",
                parameters=genai.protos.Schema(
                    type=genai.protos.Type.OBJECT,
                    properties={
                        "file_name": genai.protos.Schema(
                            type=genai.protos.Type.STRING,
                            description="Name of the Excel file",
                        ),
                        "query": genai.protos.Schema(
                            type=genai.protos.Type.STRING,
                            description="Pandas query to execute",
                        ),
                        "sheet_name": genai.protos.Schema(
                            type=genai.protos.Type.STRING,
                            description="Sheet name to query",
                        ),
                    },
                    required=["file_name", "query"],
                ),
            ),
            genai.protos.FunctionDeclaration(
                name="complex_duckdb_query",
                description="Execute complex SQL operations including multi-sheet analysis",
                parameters=genai.protos.Schema(
                    type=genai.protos.Type.OBJECT,
                    properties={
                        "file_name": genai.protos.Schema(
                            type=genai.protos.Type.STRING,
                            description="Name of the Excel file",
                        ),
                        "query": genai.protos.Schema(
                            type=genai.protos.Type.STRING,
                            description="SQL query to execute",
                        ),
                    },
                    required=["file_name", "query"],
                ),
            ),
        ]
    )
]


def generate_analysis(user_question: str, query_result: dict, query: str, conversation_history: list = None) -> str:
    """Generate intelligent analysis of query results using Gemini
    
    Args:
        user_question: Current user question
        query_result: Results from the executed query
        query: The SQL/Pandas query that was executed
        conversation_history: Previous messages for context
    """
    try:
        # Format the results for analysis
        if "error" in query_result:
            # Return user-friendly error instead of technical details
            logger.error(f"Query error in analysis: {query_result['error']}")
            return get_user_friendly_error_message()

        if "result" not in query_result:
            return "Query executed successfully but no data was returned for analysis."

        result_data = query_result["result"]

        # Get current date for context
        now = datetime.now()
        current_date = now.strftime("%B %d, %Y")
        current_month = now.strftime("%B %Y")
        current_year = str(now.year)

        # Include conversation history if available
        conversation_context = ""
        if conversation_history and len(conversation_history) > 0:
            conversation_context = "PREVIOUS CONVERSATION:\n"
            for msg in conversation_history[-6:]:  # Include last 6 messages (3 exchanges)
                role = msg.get("role", "unknown")
                content = msg.get("content", "")
                conversation_context += f"{role.upper()}: {content}\n\n"
            conversation_context += "---END OF PREVIOUS CONVERSATION---\n\n"

        # Prepare context for analysis
        analysis_context = f"""
            {conversation_context}User Question: {user_question}

            Executed Query: {query}

            Query Results:
            {json.dumps(result_data, indent=2)}

            Please provide a comprehensive business analysis of these results.
        """

        # Initialize Gemini model
        model = genai.GenerativeModel("gemini-2.5-flash")

        # Create the prompt with system instruction and current date context
        # Use .replace() instead of .format() to avoid conflicts with JSON examples
        prompt_with_dates = ANALYSIS_GENERATION_PROMPT.replace(
            "{current_date}", current_date
        )
        prompt_with_dates = prompt_with_dates.replace("{current_month}", current_month)
        prompt_with_dates = prompt_with_dates.replace("{current_year}", current_year)
        full_prompt = prompt_with_dates + f"\n\n{analysis_context}"

        response = model.generate_content(full_prompt)

        return response.text or "Unable to generate analysis."

    except Exception as e:
        logger.error(f"Error generating analysis: {str(e)}")
        return f"Error generating analysis: {str(e)}"


def execute_function(name: str, args: dict, state: AgentState) -> dict:
    """Execute the appropriate function based on name"""
    try:
        function_map = {
            "load_preview_data": load_preview_data,
            "simple_dataframe_query": simple_dataframe_query,
            "complex_duckdb_query": complex_duckdb_query,
        }

        result = function_map[name](**args)

        if isinstance(result, dict) and "error" in result:
            state["error"] = result["error"]

        return result

    except Exception as e:
        error_msg = f"Function execution error: {str(e)}"
        logger.error(error_msg)
        state["error"] = get_user_friendly_error_message()
        return {"error": get_user_friendly_error_message()}


def analysis_generation_node(state: AgentState) -> AgentState:
    """Generate final analysis and insights from query results"""
    try:
        if state.get("error") or not state.get("query_result"):
            return state

        print("Generating intelligent analysis...")

        analysis = generate_analysis(
            user_question=state["user_input"],
            query_result=state["query_result"],
            query=state.get("query", ""),
            conversation_history=state.get("messages", []),
        )

        state["final_analysis"] = analysis
        state["workflow_stage"] = "completed"  # Mark as completed
        return state

    except Exception as e:
        logger.error(f"Error in analysis_generation_node: {str(e)}")
        state["error"] = f"Analysis generation error: {str(e)}"
        return state


def generate_and_execute_query_node(state: AgentState) -> AgentState:
    """Generate and execute initial query using Gemini"""
    try:
        if not state.get("file_name") or not state.get("user_input"):
            raise ValueError("Missing required fields: file_name or user_input")

        state["iterations_count"] = state.get("iterations_count", 0) + 1

        if "messages" not in state:
            state["messages"] = []

        # Improve message formatting to be more explicit
        # Include conversation history if available
        conversation_context = ""
        if state.get("messages") and len(state["messages"]) > 0:
            conversation_context = "\nCONVERSATION HISTORY:\n"
            for msg in state["messages"][-6:]:  # Include last 6 messages for context
                role = msg.get("role", "unknown")
                content = msg.get("content", "")
                conversation_context += f"{role.upper()}: {content}\n\n"
            conversation_context += "---END OF CONVERSATION HISTORY---\n\n"
        
        user_message = f"""{conversation_context}File: {state['file_name']}
            User Question: {state['user_input']}
            Preview Data Available: {bool(state.get('preview_data'))}

            {f"Preview Data: {json.dumps(state.get('preview_data'), indent=2)}" if state.get('preview_data') else "No preview data available - you must call load_preview_data first"}

            You MUST call a function to handle this request.
        """

        if (
            state.get("iterations_count", 0) <= 3
        ):  # Only show debug info for first few iterations
            print(f"\nIteration {state.get('iterations_count')} - Generating Query...")

        # Get current date for context
        now = datetime.now()
        current_date = now.strftime("%B %d, %Y")
        current_month = now.strftime("%B %Y")
        current_year = str(now.year)

        # Initialize Gemini model with tools
        model = genai.GenerativeModel("gemini-2.5-flash", tools=TOOLS)

        # Create the full prompt with current date context
        # Use replace() instead of format() to avoid issues with JSON examples in prompt
        prompt_with_dates = QUERY_GENERATION_PROMPT.replace(
            "{current_date}", current_date
        )
        prompt_with_dates = prompt_with_dates.replace("{current_month}", current_month)
        prompt_with_dates = prompt_with_dates.replace("{current_year}", current_year)
        full_prompt = prompt_with_dates + f"\n\n{user_message}"

        response = model.generate_content(full_prompt)

        # Debug logging
        logger.info(f"Gemini response type: {type(response)}")
        try:
            logger.info(f"Gemini response: {response}")
        except Exception as log_error:
            logger.error(f"Could not log response: {log_error}")

        # Handle function calls in Gemini response
        try:
            if hasattr(response, "candidates") and response.candidates:
                candidate = response.candidates[0]
                logger.info(f"Candidate: {candidate}")

                if hasattr(candidate, "content") and candidate.content:
                    logger.info(f"Content: {candidate.content}")

                    if hasattr(candidate.content, "parts") and candidate.content.parts:
                        for part in candidate.content.parts:
                            logger.info(f"Part: {part}")
                            logger.info(f"Part type: {type(part)}")

                            if hasattr(part, "function_call") and part.function_call:
                                function_call = part.function_call
                                function_name = function_call.name
                                logger.info(f"Function call: {function_name}")

                                # Convert function args properly
                                function_args = {}
                                if (
                                    hasattr(function_call, "args")
                                    and function_call.args
                                ):
                                    try:
                                        # Gemini returns args as a dict-like object
                                        # Convert to regular dict
                                        if hasattr(function_call.args, "items"):
                                            # If it has items() method, use it
                                            for (
                                                key,
                                                value,
                                            ) in function_call.args.items():
                                                function_args[str(key)] = (
                                                    str(value)
                                                    if value is not None
                                                    else None
                                                )
                                        else:
                                            # Try direct dict conversion
                                            function_args = dict(function_call.args)
                                            # Ensure all values are strings or None
                                            function_args = {
                                                str(k): (
                                                    str(v) if v is not None else None
                                                )
                                                for k, v in function_args.items()
                                            }
                                    except Exception as args_error:
                                        logger.error(
                                            f"Error converting function args: {args_error}"
                                        )
                                        logger.error(
                                            f"Args type: {type(function_call.args)}"
                                        )
                                        logger.error(
                                            f"Args content: {function_call.args}"
                                        )
                                        # Fallback: try to extract args manually by converting to string and parsing
                                        try:
                                            import re

                                            args_str = str(function_call.args)
                                            logger.info(
                                                f"Attempting to parse args from string: {args_str}"
                                            )
                                        except:
                                            pass
                                        function_args = {}

                                logger.info(f"Function args: {function_args}")

                                # Execute the function
                                result = execute_function(
                                    function_name, function_args, state
                                )

                                # Handle different function types
                                if function_name == "load_preview_data":
                                    # Store preview data and continue to generate actual query
                                    state["preview_data"] = result
                                    state["tool"] = function_name
                                    if "error" in result:
                                        state["error"] = result["error"]
                                        state["workflow_stage"] = "error"
                                    else:
                                        # Continue to generate query with preview data now available
                                        state["workflow_stage"] = "generate_query"
                                else:
                                    # For actual query functions, store results normally
                                    state["query"] = function_args.get("query", "")
                                    state["tool"] = function_name
                                    state["query_result"] = result

                                    # Check if query succeeded or failed
                                    if "error" in result:
                                        state["workflow_stage"] = "error"
                                    else:
                                        state["workflow_stage"] = "analysis_ready"

                                return state
        except Exception as parse_error:
            logger.error(f"Error parsing Gemini response: {parse_error}")
            import traceback

            logger.error(f"Traceback: {traceback.format_exc()}")
            raise parse_error

        # If no function call was made, this is an error
        logger.error("Model did not call any function as required")
        state["error"] = get_user_friendly_error_message()
        return state

    except Exception as e:
        logger.error(f"Error in generate_and_execute_query_node: {str(e)}")
        state["error"] = get_user_friendly_error_message()
        return state


# Workflow routing functions
def should_continue_to_analysis(state: AgentState) -> str:
    """Determine if query succeeded and ready for analysis"""
    # Check if there's an error
    if state.get("error"):
        return END
    
    # Check if query result has an error
    if state.get("query_result") and "error" in state["query_result"]:
        # Query failed - return error and end
        error_msg = state["query_result"]["error"]
        logger.error(f"Query execution failed: {error_msg}")
        state["error"] = get_user_friendly_error_message()
        return END
    
    # Check if we have a successful query result
    if state.get("query_result") and "result" in state["query_result"]:
        return "analyze"
    
    # Check if preview data was loaded (need to loop back to generate query)
    if state.get("workflow_stage") == "generate_query":
        return "generate"
    
    # Default: something went wrong
    logger.error("Unexpected workflow state")
    state["error"] = get_user_friendly_error_message()
    return END


def should_continue_after_analysis(state: AgentState) -> str:
    """Determine if workflow should continue or end after analysis"""
    if state.get("error"):
        return END
    elif state.get("workflow_stage") == "completed":
        return END
    elif state.get("iterations_count", 0) > 10:
        logger.error("Maximum iterations (10) reached")
        state["error"] = get_user_friendly_error_message()
        return END
    else:
        return "continue"


# Create the workflow graph
def create_workflow():
    """Create the simplified LangGraph workflow (no validation)"""
    workflow = StateGraph(AgentState)

    # Add nodes (removed validate_query)
    workflow.add_node("generate_query", generate_and_execute_query_node)
    workflow.add_node("generate_analysis", analysis_generation_node)

    # Set entry point
    workflow.set_entry_point("generate_query")

    # Add conditional edges
    workflow.add_conditional_edges(
        "generate_query",
        should_continue_to_analysis,
        {
            "generate": "generate_query",  # Loop back if preview data was loaded
            "analyze": "generate_analysis",  # Go to analysis if query succeeded
            END: END,  # End if error
        },
    )

    workflow.add_conditional_edges(
        "generate_analysis", 
        should_continue_after_analysis, 
        {
            "continue": "generate_query",
            END: END
        }
    )

    # Compile the workflow
    memory = MemorySaver()
    app = workflow.compile(checkpointer=memory)

    return app


def run_excel_analysis(file_name: str, user_question: str, session_id: str = None, conversation_history: list = None) -> str:
    """Main function to run Excel analysis using Gemini
    
    Args:
        file_name: Path to Excel file
        user_question: Current user question
        session_id: Unique session identifier for conversation memory
        conversation_history: List of previous messages in format [{"role": "user/assistant", "content": "..."}]
    """
    try:
        print(f"🔍 Analyzing Excel file: {file_name}")
        print(f"📝 Question: {user_question}")
        if session_id:
            print(f"💬 Session ID: {session_id}")
            print(f"📚 Conversation history: {len(conversation_history) if conversation_history else 0} messages")
        print("=" * 50)

        # Create workflow
        app = create_workflow()

        # Initial state
        initial_state = {
            "user_input": user_question,
            "file_name": file_name,
            "sheet_name": None,
            "preview_data": None,
            "query_result": None,
            "final_analysis": None,
            "llm_prompt": None,
            "tool": None,
            "iterations_count": 0,
            "error": None,
            "messages": conversation_history if conversation_history else [],
            "workflow_stage": "initial",
        }

        # Run the workflow with session-specific thread_id for memory persistence
        # Use session_id if provided, otherwise use a default thread_id
        thread_id = session_id if session_id else "excel_analysis_thread"
        config = {"configurable": {"thread_id": thread_id}}
        final_state = app.invoke(initial_state, config)

        # Return results
        if final_state.get("error"):
            # Don't expose technical error details to user
            logger.error(f"Analysis failed with error: {final_state['error']}")
            return final_state['error']  # Already user-friendly from get_user_friendly_error_message()
        elif final_state.get("final_analysis"):
            return final_state["final_analysis"]
        else:
            return get_user_friendly_error_message()

    except Exception as e:
        logger.error(f"Error in run_excel_analysis: {str(e)}")
        return get_user_friendly_error_message()


# Main execution
if __name__ == "__main__":
    # Example usage
    if len(sys.argv) > 1:
        question = " ".join(sys.argv[1:])
    else:
        question = "What data is available in this Excel file?"

    result = run_excel_analysis("data-simplified.xlsx", question)
    print("\n" + "=" * 50)
    print("📊 ANALYSIS RESULTS")
    print("=" * 50)
    print(result)