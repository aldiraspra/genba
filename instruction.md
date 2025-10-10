# Smart Branch Assistant - Excel Analysis AI

AI-powered Excel data analysis assistant using Google Gemini and LangGraph.

## Features

- 🤖 **Intelligent Analysis**: Natural language querying of Excel data
- 💬 **Chat Interface**: Conversational interface with message history
- 📊 **Multi-Sheet Support**: Analyze data across multiple Excel sheets
- ⚡ **Fast Error Reporting**: Immediate error feedback with clear messages
- 💾 **Session Management**: Save and resume conversation sessions
- 🇮🇩 **Indonesian Support**: Fully supports Indonesian language queries

## Installation

1. Install required packages:

```bash
python -m venv .venv
.venv\Scripts\Activate
pip install -r requirements.txt
```

2. Set up your environment variables:
   Create a `.env` file in the project directory:

```
GOOGLE_API_KEY=your_gemini_api_key_here
```

## Running the Application

### Option 1: Streamlit UI (Recommended)

```bash
streamlit run app.py
```

The app will open in your browser at `http://localhost:8501`

### Option 2: Command Line

```bash
python gemini.py "Your question here"
```

Example:

```bash
python gemini.py "Berapa revenue traga di bulan Juli?"
```

## Project Structure

```
genbafc/
├── app.py                    # Streamlit UI (Frontend)
├── gemini.py                 # Backend logic (Analysis engine)
├── data-simplified.xlsx      # Data source
├── chat_sessions.db          # SQLite database (auto-created)
├── requirements.txt          # Python dependencies
├── .env                      # Environment variables
└── README.md                # This file
```

## Usage Examples

### Indonesian Queries:

- "Berapa revenue traga di bulan agustus?"
- "Tunjukkan sales performance untuk bulan Juli"
- "Unit sales mana yang memberikan sumbangan revenue tertinggi?"

### English Queries:

- "What is the total revenue in July 2025?"
- "Show me the top performing unit sales"
- "Compare service vs parts revenue"

## Features in Detail

### 1. Session Management

- Conversations are automatically saved
- Click on past sessions to resume
- Sessions are titled with your first question
- Edit session titles by clicking on them

### 2. Smart Analysis

- Automatically detects data types
- Handles comma-formatted numbers
- Cleans dirty data automatically
- Provides business insights and recommendations

### 3. Multi-Sheet Analysis

- Automatically discovers all sheets in Excel
- Supports complex SQL queries with JOINs
- Sanitizes table names for SQL compatibility

## Technical Details

### Backend (`gemini.py`)

- **AI Model**: Google Gemini 2.5 Flash
- **Workflow**: LangGraph state machine (2-node simplified workflow)
- **Query Engine**: DuckDB for SQL, Pandas for simple operations
- **Error Handling**: Fast error reporting with immediate feedback
- **Caching**: DuckDB connection caching for improved performance

### Frontend (`app.py`)

- **Framework**: Streamlit
- **Chat Component**: streamlit-chat
- **Database**: SQLite for session persistence
- **UI Theme**: Custom CSS matching Smart Branch Assistant design

## Data Format

The Excel file (`data-simplified.xlsx`) contains 9 sheets:

1. SPK DO - Sales/Delivery Orders
2. Summary Sales Funneling
3. SUS Plan Tahun
4. EUS Plan Bulanan
5. Sales Performance
6. Service Performance
7. Part Performance
8. Financial Performance
9. Manpower Performance

## Troubleshooting

### Issue: ModuleNotFoundError

**Solution**: Install dependencies

```bash
pip install -r requirements.txt
```

### Issue: API Key Error

**Solution**: Ensure `.env` file has valid GOOGLE_API_KEY

### Issue: Database Lock

**Solution**: Close other instances of the app, delete `chat_sessions.db` to reset

## Development

### Backend Only Testing

```bash
python gemini.py "test question"
```

### Examine Data Structure

```bash
python examine_data.py
```

## License

Internal use only.

## Support

For issues or questions, contact the development team.
