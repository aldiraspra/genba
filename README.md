# Excel Analysis Agent with Gemini AI

An intelligent Excel data analysis system that converts natural language questions into SQL/Pandas queries, executes them, and generates business insights using Google's Gemini AI and LangGraph.

## 🎯 What It Does

Ask questions in plain English about your Excel data, and get:

- ✅ Automated SQL/Pandas query generation
- ✅ Multi-sheet analysis with JOINs
- ✅ Business insights and recommendations
- ✅ Conversation memory for follow-up questions
- ✅ Fast error reporting (no auto-retry)

**Example:**

```
You: "What's the total revenue for July 2025?"
Agent: Generates SQL → Executes → Returns analysis with insights
```

## 🏗️ Architecture

```
┌─────────────────┐
│  User Question  │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────┐
│              LangGraph Workflow                         │
│                                                         │
│  ┌──────────────┐    ┌──────────┐                     │
│  │   Generate   │───▶│ Generate │                     │
│  │    Query     │    │ Analysis │                     │
│  └──────────────┘    └──────────┘                     │
│         │                    │                        │
│         ▼                    ▼                        │
│  ┌──────────────────────────────────────────────────┐ │
│  │         Gemini AI (Function Calling)            │ │
│  └──────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────┐
│              Execution Layer                            │
│                                                         │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │ Load Preview │  │    Pandas    │  │   DuckDB     │ │
│  │     Data     │  │    Query     │  │  SQL Query   │ │
│  └──────────────┘  └──────────────┘  └──────────────┘ │
└─────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────┐
│  Excel File(s)  │
└─────────────────┘
```

## 🔄 Workflow Explained

### Step 1: Query Generation

```python
User Input → Gemini AI → Function Call Decision
```

- Gemini analyzes the question
- Decides which function to call:
  - `load_preview_data()` - First time analyzing a file
  - `simple_dataframe_query()` - Simple single-sheet operations
  - `complex_duckdb_query()` - Complex multi-sheet SQL queries

### Step 2: Query Execution

```python
Function Call → Execute → Return Results
```

- **Preview Data**: Loads Excel structure (sheets, columns, types)
- **Pandas Query**: Executes Python code on DataFrame
- **DuckDB Query**: Executes SQL across multiple sheets
- **Error Handling**: If query fails, error is reported immediately (no auto-retry)

### Step 3: Analysis Generation

```python
Query Results → Gemini AI → Business Insights
```

- Generates executive summary
- Identifies patterns and trends
- Provides actionable recommendations
- Highlights risks and concerns

## 📁 Project Structure

```
genbafc/
├── gemini.py              # Main application
├── .env                   # API keys (GOOGLE_API_KEY)
├── data-simplified.xlsx   # Sample Excel file
└── README.md             # This file
```

## 🚀 Quick Start

### 1. Installation

```bash
# Install dependencies
pip install pandas duckdb google-generativeai langgraph python-dotenv openpyxl
```

### 2. Configuration

Create `.env` file:

```env
GOOGLE_API_KEY=your_gemini_api_key_here
```

### 3. Usage

**Command Line:**

```bash
python gemini.py "What's the total revenue this month?"
```

**Python Code:**

```python
from gemini import run_excel_analysis

# Simple query
result = run_excel_analysis(
    file_name="data.xlsx",
    user_question="Show me top 5 products by sales"
)
print(result)

# With conversation history
result = run_excel_analysis(
    file_name="data.xlsx",
    user_question="What about last month?",
    session_id="user123",
    conversation_history=[
        {"role": "user", "content": "Show revenue for July"},
        {"role": "assistant", "content": "July revenue was..."}
    ]
)
```

## 🔧 Core Components

### 1. State Management (AgentState)

```python
AgentState = {
    "user_input": str,           # User's question
    "file_name": str,            # Excel file path
    "preview_data": dict,        # Excel structure info
    "query": str,                # Generated SQL/Pandas query
    "query_result": dict,        # Execution results
    "final_analysis": str,       # AI-generated insights
    "messages": list,            # Conversation history
    "workflow_stage": str,       # Current stage
    "error": str                 # Error message if any
}
```

### 2. Main Functions

**a) load_preview_data(file_name, sheet_name)**

- Reads Excel file structure
- Returns sheets, columns, data types, sample rows
- Cached for performance

**b) simple_dataframe_query(file_name, query, sheet_name)**

- Executes Pandas operations
- For single-sheet analysis
- Example: `df.head(10)`, `df[df['Sales'] > 1000]`

**c) complex_duckdb_query(file_name, query)**

- Executes SQL queries
- Supports multi-sheet JOINs
- Handles complex aggregations
- Connection caching for speed

### 3. LangGraph Nodes

**generate_and_execute_query_node**

- Sends question + context to Gemini
- Receives function call
- Executes function
- Updates state

**analysis_generation_node**

- Takes successful query results
- Generates business analysis
- Returns formatted insights

## 🎨 Key Features

### 1. Conversation Memory

```python
# Remembers context across questions
Q1: "Show July revenue"
Q2: "What about August?"  # Understands context
Q3: "Compare them"        # Knows what to compare
```

### 2. Date Context Awareness

```python
# Automatically understands relative dates
"revenue this month"  → Uses current month
"sales today"         → Uses today's date
"last quarter"        → Calculates quarter
```

### 3. Multi-Sheet Analysis

```python
# Automatically JOINs sheets
"Compare sales from Sheet1 with targets from Sheet2"
→ Generates: SELECT s.*, t.target
             FROM sheet1 s
             JOIN sheet2 t ON s.id = t.id
```

### 4. Smart Data Cleaning

```python
# Handles messy Excel data
"4,665"     → 4665      (removes commas)
"-"         → NULL      (handles dashes)
"  123  "   → 123       (trims whitespace)
"N/A"       → NULL      (handles text nulls)
```

### 5. Connection Caching

```python
# Reuses DuckDB connections
First query:  Creates connection + registers sheets
Next queries: Reuses cached connection (faster!)
```

## 📊 Example Queries

### Simple Queries

```python
"Show first 10 rows"
"What columns are available?"
"Count total records"
```

### Aggregations

```python
"Total revenue by month"
"Average sales per product"
"Top 5 customers by revenue"
```

### Multi-Sheet Analysis

```python
"Compare actual vs target from different sheets"
"Join sales data with customer information"
"Calculate conversion rate across sheets"
```

### Time-Based Analysis

```python
"Revenue trend for last 6 months"
"Month-over-month growth"
"Year-to-date performance"
```

## 🛠️ Advanced Configuration

### Custom Prompts

Modify these constants in `gemini.py`:

- `QUERY_GENERATION_PROMPT` - Controls query generation
- `ANALYSIS_GENERATION_PROMPT` - Controls insights generation

### Cache Management

```python
from gemini import clear_duckdb_cache

# Clear specific file cache
clear_duckdb_cache("data.xlsx")

# Clear all caches
clear_duckdb_cache()
```

### Logging

```python
import logging

# Set log level
logging.getLogger("gemini").setLevel(logging.DEBUG)
```

## 🐛 Troubleshooting

### Common Issues

**1. "Table does not exist" error**

- Cause: Sheet name has spaces
- Fix: Uses sanitized names automatically (e.g., "Sales Data" → `sales_data`)

**2. "GROUP BY" errors**

- Cause: Non-aggregated columns in SELECT
- Fix: Add missing columns to GROUP BY manually

**3. Type conversion errors**

- Cause: Dirty data (commas, dashes, text)
- Fix: Automatic data cleaning in queries

**4. API rate limits**

- Cause: Too many Gemini API calls
- Fix: Implement rate limiting (see optimization suggestions)

## 📈 Performance Tips

1. **Use caching**: Don't clear cache unnecessarily
2. **Limit preview rows**: Default is 3 rows (configurable)
3. **Batch questions**: Ask multiple questions in one session
4. **Optimize queries**: Let AI generate efficient SQL
5. **Monitor iterations**: High iteration count = complex question

## 🔐 Security Notes

- Never commit `.env` file with API keys
- Validate file paths to prevent directory traversal
- Sanitize user inputs (currently uses safe eval for Pandas)
- Use read-only Excel files when possible

## 🤝 Contributing

To extend functionality:

1. Add new functions in execution layer
2. Register functions in `TOOLS` array
3. Update prompts to include new capabilities
4. Test with various Excel formats

## 📝 License

[Your License Here]

## 🙏 Acknowledgments

- **Google Gemini AI** - LLM and function calling
- **LangGraph** - Workflow orchestration
- **DuckDB** - Fast SQL analytics
- **Pandas** - Data manipulation

## 📞 Support

For issues or questions:

- Check logs for detailed error messages
- Review prompt templates for customization
- Increase logging level for debugging

---
