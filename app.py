import streamlit as st
from streamlit_chat import message
import sqlite3
from datetime import datetime, timedelta
import uuid
from gemini import run_excel_analysis
import logging

# Configure logging to only show in terminal
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Page config
st.set_page_config(
    page_title="Smart Branch Assistant",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS matching the reference image
st.markdown(
    """
<style>
    html, body, .main, .block-container {
        font-size: 0.85rem !important; /* adjust 0.8–0.9 */
    }

    /* Main background */
    .main {
        background-color: #F5F7F9;
    }
    
    /* Reduce top padding of main content area */
    .block-container {
        padding-top: 0rem !important;
    }
    
    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background-color: #FFFFFF;
        padding: 0.75rem !important;
        font-size: 0.85rem !important;
    }
    
    /* Logo and title styling */
    .logo-container {
        display: flex;
        align-items: center;
        padding: 1rem 0;
        margin-bottom: 2rem;
        border-bottom: 1px solid #E5E9F0;
    }
    
    .logo-icon {
        background-color: #004996;
        color: white;
        padding: 0.5rem;
        border-radius: 8px;
        font-size: 1.5rem;
        margin-right: 0.75rem;
    }
    
    .logo-text h3 {
        margin: 0;
        color: #004996;
        font-size: 1.1rem;
        font-weight: 600;
    }
    
    .logo-text p {
        margin: 0;
        color: #6B7280;
        font-size: 0.75rem;
    }
    
    /* Session history styling */
    .session-item {
        padding: 0.75rem;
        margin: 0.5rem 0;
        border-radius: 8px;
        cursor: pointer;
        background-color: #F8FAFC;
        border: 1px solid #E5E9F0;
        transition: all 0.2s;
    }
    
    .session-item:hover {
        background-color: #BABFE0;
        border-color: #004996;
    }
    
    .session-item.active {
        background-color: #BABFE0;
        border-color: #004996;
    }
    
    .session-title {
        font-size: 0.9rem;
        font-weight: 500;
        color: #004996;
        margin-bottom: 0.25rem;
    }
    
    .session-time {
        font-size: 0.75rem;
        color: #6B7280;
    }
    
    /* New Analysis button */
    .stButton > button {
        width: 100%;
        background-color: #004996;
        color: white;
        border: none;
        padding: 0.5rem 0.6rem !important;
        border-radius: 8px;
        font-weight: 500;
        transition: all 0.2s;
        font-size: 0.85rem !important;
        margin-bottom: -1rem !important;
    }
    
    .stButton > button:hover {
        background-color: #2C5282;
    }
    
    /* Chat container */
    .chat-container {
        max-width: 900px;
        margin: -2rem auto 0 auto;
        padding: 0 1rem;
    }
    
    /* Chat messages */
    .stChatMessage {
        font-size: 0.9rem !important;
        line-height: 1.4;
    }
    
    /* Message bubbles - User */
    .stChatMessage[data-testid="user-message"] {
        background-color: #004996;
        color: white;
        border-radius: 12px;
        padding: 0.75rem 1rem;
        margin: 0.5rem 0;
        margin-left: 20%;
    }
    
    /* Message bubbles - Assistant */
    .stChatMessage[data-testid="assistant-message"] {
        background-color: #FFFFFF;
        color: #1F2937;
        border-radius: 12px;
        padding: 0.75rem 1rem;
        margin: 0.5rem 0;
        margin-right: 20%;
        border: 1px solid #E5E9F0;
    }
    
    /* Timestamp styling */
    .message-time {
        font-size: 0.7rem;
        color: #9CA3AF;
        margin-top: 0.25rem;
    }
    
    /* Suggested questions pills */
    .suggestion-pill {
        display: inline-block;
        padding: 0.5rem 1rem;
        margin: 0.25rem;
        background-color: #F8FAFC;
        border: 1px solid #004996;
        border-radius: 20px;
        color: #004996;
        font-size: 0.85rem;
        cursor: pointer;
        transition: all 0.2s;
    }
    
    .suggestion-pill:hover {
        background-color: #004996;
        color: white;
    }
    
    /* Chat input area */
    .stChatInputContainer {
        border-top: 1px solid #E5E9F0;
        padding-top: 1rem;
    }
    
    .stChatInputContainer textarea{
        font-size: 0.9rem !important;
        padding: 0.6rem !important;
    }
    
    /* Limit width for better balance */
    .block-container {
        max-width: 1200px !important; /* narrower page */
        margin: auto;
    }
    
    /* Hide streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    
    /* Session menu styling */
    .session-container {
        display: flex;
        align-items: center;
        justify-content: space-between;
    }
    
    /* Truncate long session titles */
    .stButton > button {
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        text-align: left;
    }
    
    /* Welcome message */
    .welcome-message {
        background-color: white;
        padding: 2rem;
        border-radius: 12px;
        margin: 2rem auto;
        max-width: 700px;
        border: 1px solid #E5E9F0;
        font-size: 0.9rem !important;
    }
    
    .welcome-title {
        color: #004996;
        font-size: 0.9rem;
        font-weight: 600;
        margin-bottom: 1rem;
    }
    
    .welcome-subtitle {
        color: #6B7280;
        font-size: 0.95rem;
        margin-bottom: 1.5rem;
    }
    
    
</style>
""",
    unsafe_allow_html=True,
)


# Database setup
def init_db():
    """Initialize SQLite database for session management"""
    try:
        conn = sqlite3.connect("data/chat_sessions.db")
        c = conn.cursor()

        # Create sessions table
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS sessions (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
        """
        )

        # Create messages table
        c.execute(
            """
            CREATE TABLE IF NOT EXISTS messages (
                id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                FOREIGN KEY (session_id) REFERENCES sessions (id)
            )
        """
        )

        conn.commit()
        conn.close()
        logger.info("✅ Database initialized successfully")
    except Exception as e:
        logger.error(f"❌ Database initialization failed: {e}")
        # Don't crash the app - session history won't work but queries will
        pass


def safe_db_operation(operation_func, *args, **kwargs):
    """Wrapper for database operations that won't crash the app"""
    try:
        return operation_func(*args, **kwargs)
    except Exception as e:
        logger.error(f"Database operation failed: {e}")
        return None


def create_session(title="New Analysis"):
    """Create a new session"""
    session_id = str(uuid.uuid4())
    now = datetime.now().isoformat()

    conn = sqlite3.connect("data/chat_sessions.db")
    c = conn.cursor()
    c.execute(
        "INSERT INTO sessions (id, title, created_at, updated_at) VALUES (?, ?, ?, ?)",
        (session_id, title, now, now),
    )
    conn.commit()
    conn.close()

    return session_id


def update_session_title(session_id, title):
    """Update session title"""
    conn = sqlite3.connect("data/chat_sessions.db")
    c = conn.cursor()
    c.execute(
        "UPDATE sessions SET title = ?, updated_at = ? WHERE id = ?",
        (title, datetime.now().isoformat(), session_id),
    )
    conn.commit()
    conn.close()


def get_session_title(session_id):
    """Get the title of a specific session"""
    if not session_id:
        return None
    conn = sqlite3.connect("data/chat_sessions.db")
    c = conn.cursor()
    c.execute("SELECT title FROM sessions WHERE id = ?", (session_id,))
    result = c.fetchone()
    conn.close()
    return result[0] if result else None


def get_all_sessions():
    """Get all sessions ordered by updated_at"""
    conn = sqlite3.connect("data/chat_sessions.db")
    c = conn.cursor()
    c.execute(
        "SELECT id, title, created_at, updated_at FROM sessions ORDER BY updated_at DESC"
    )
    sessions = c.fetchall()
    conn.close()
    return sessions


def get_session_messages(session_id):
    """Get all messages for a session"""
    conn = sqlite3.connect("data/chat_sessions.db")
    c = conn.cursor()
    c.execute(
        "SELECT id, role, content, timestamp FROM messages WHERE session_id = ? ORDER BY timestamp ASC",
        (session_id,),
    )
    messages = c.fetchall()
    conn.close()
    return messages


def add_message(session_id, role, content):
    """Add a message to a session"""
    message_id = str(uuid.uuid4())
    now = datetime.now().isoformat()

    conn = sqlite3.connect("data/chat_sessions.db")
    c = conn.cursor()
    c.execute(
        "INSERT INTO messages (id, session_id, role, content, timestamp) VALUES (?, ?, ?, ?, ?)",
        (message_id, session_id, role, content, now),
    )
    # Update session's updated_at
    c.execute("UPDATE sessions SET updated_at = ? WHERE id = ?", (now, session_id))
    conn.commit()
    conn.close()


def delete_session(session_id):
    """Delete a session and all its messages"""
    conn = sqlite3.connect("data/chat_sessions.db")
    c = conn.cursor()
    # Delete messages first
    c.execute("DELETE FROM messages WHERE session_id = ?", (session_id,))
    # Delete session
    c.execute("DELETE FROM sessions WHERE id = ?", (session_id,))
    conn.commit()
    conn.close()


def format_timestamp(iso_timestamp):
    """Format ISO timestamp to HH:MM"""
    dt = datetime.fromisoformat(iso_timestamp)
    return dt.strftime("%H:%M")


def format_timestamp(iso_timestamp):
    """Format ISO timestamp for session list"""
    dt = datetime.fromisoformat(iso_timestamp)
    now = datetime.now()

    if dt.date() == now.date():
        return dt.strftime("%H:%M")
    elif dt.date() == (now - timedelta(days=1)).date():
        return "Yesterday"
    else:
        return dt.strftime("%b %d")


# Initialize database (with error handling)
init_db()

# Function to initialize session state
def init_session_state():
    """Initialize session state variables"""
    if "current_session_id" not in st.session_state:
        # Don't create session yet - wait for first message
        st.session_state.current_session_id = None
        st.session_state.messages = []
        st.session_state.first_message = True

    # Initialize pagination state
    if "session_page" not in st.session_state:
        st.session_state.session_page = 0

# Call initialization when app runs
init_session_state()

SESSIONS_PER_PAGE = 5  # Number of sessions to show per page

# Sidebar
with st.sidebar:
    # Logo and title
    st.markdown(
        """
    <div class="logo-container">
        <div class="logo-icon">📈</div>
        <div class="logo-text">
            <h3>Smart Branch Assistant</h3>
            <p>AI-Driven Agent Analyst</p>
        </div>
    </div>
    """,
        unsafe_allow_html=True,
    )

    # New Analysis button - centered at top
    col1, col2, col3 = st.columns([1, 7, 1])
    with col2:
        if st.button("➕ New Analysis", key="new_analysis", use_container_width=True):
            # Don't create session yet - just reset state
            st.session_state.current_session_id = None
            st.session_state.messages = []
            st.session_state.first_message = True
            st.rerun()

    # Session history
    st.markdown("---")

    sessions = get_all_sessions()
    total_sessions = len(sessions)
    total_pages = (
        (total_sessions + SESSIONS_PER_PAGE - 1) // SESSIONS_PER_PAGE
        if total_sessions > 0
        else 1
    )

    # Ensure page is within bounds
    if st.session_state.session_page >= total_pages:
        st.session_state.session_page = max(0, total_pages - 1)

        st.markdown("---")

    # Calculate slice for current page
    start_idx = st.session_state.session_page * SESSIONS_PER_PAGE
    end_idx = start_idx + SESSIONS_PER_PAGE
    page_sessions = sessions[start_idx:end_idx]

    if page_sessions:
        if page_sessions:
            for session_id, title, created_at, updated_at in page_sessions:
                is_active = session_id == st.session_state.current_session_id

                # Truncate title for display (like Perplexity)
                display_title = title[:25] + "..." if len(title) > 40 else title

                # Create container for each session with menu
                col1, col2 = st.columns([5, 1])

                with col1:
                    if st.button(
                        display_title,
                        key=f"session_{session_id}",
                        use_container_width=True,
                        type="primary" if is_active else "secondary",
                    ):
                        # Load session
                        st.session_state.current_session_id = session_id
                        # Load messages from database
                        messages = get_session_messages(session_id)
                        st.session_state.messages = [
                            {"role": role, "content": content, "timestamp": timestamp}
                            for _, role, content, timestamp in messages
                        ]
                        st.session_state.first_message = False
                        st.rerun()

                with col2:
                    # Ellipsis menu button
                    if st.button("⋮", key=f"menu_{session_id}", help="Options"):
                        st.session_state[f"show_menu_{session_id}"] = (
                            not st.session_state.get(f"show_menu_{session_id}", False)
                        )

                # Show menu options if button clicked
                if st.session_state.get(f"show_menu_{session_id}", False):
                    menu_col1, menu_col2 = st.columns(2)

                    with menu_col1:
                        if st.button(
                            "✏️ Rename",
                            key=f"rename_{session_id}",
                            use_container_width=True,
                        ):
                            st.session_state[f"renaming_{session_id}"] = True
                            st.session_state[f"show_menu_{session_id}"] = False
                            st.rerun()

                    with menu_col2:
                        if st.button(
                            "🗑️ Delete",
                            key=f"delete_{session_id}",
                            use_container_width=True,
                        ):
                            delete_session(session_id)
                            # If deleted session was active, reset
                            if st.session_state.current_session_id == session_id:
                                st.session_state.current_session_id = None
                                st.session_state.messages = []
                                st.session_state.first_message = True
                            st.session_state[f"show_menu_{session_id}"] = False
                            st.rerun()

                # Show rename input if renaming
                if st.session_state.get(f"renaming_{session_id}", False):
                    new_title = st.text_input(
                        "New title:", value=title, key=f"rename_input_{session_id}"
                    )
                    rename_col1, rename_col2 = st.columns(2)
                    with rename_col1:
                        if st.button(
                            "✓ Save",
                            key=f"save_rename_{session_id}",
                            use_container_width=True,
                        ):
                            if new_title.strip():
                                update_session_title(session_id, new_title.strip())
                                st.session_state[f"renaming_{session_id}"] = False
                                st.rerun()
                    with rename_col2:
                        if st.button(
                            "✗ Cancel",
                            key=f"cancel_rename_{session_id}",
                            use_container_width=True,
                        ):
                            st.session_state[f"renaming_{session_id}"] = False
                            st.rerun()

    # Pagination controls at bottom
    if total_sessions > SESSIONS_PER_PAGE:
        st.markdown("---")
        page_col1, page_col2, page_col3 = st.columns([2, 2, 2])

        with page_col1:
            if st.button(
                "◀",
                key="prev_page_bottom",
                disabled=st.session_state.session_page == 0,
                use_container_width=True,
            ):
                st.session_state.session_page -= 1
                st.rerun()

        with page_col2:
            st.markdown(
                f"<div style='text-align: center; padding: 0.5rem;'>{st.session_state.session_page + 1} of {total_pages}</div>",
                unsafe_allow_html=True,
            )

        with page_col3:
            if st.button(
                "▶",
                key="next_page_bottom",
                disabled=st.session_state.session_page >= total_pages - 1,
                use_container_width=True,
            ):
                st.session_state.session_page += 1
                st.rerun()

    elif total_sessions == 0:
        st.info("No conversation history yet.")

# Main chat area
st.markdown('<div class="chat-container">', unsafe_allow_html=True)

# Display title - dynamic based on session
if st.session_state.current_session_id:
    session_title = get_session_title(st.session_state.current_session_id)
    st.title(session_title if session_title else "New Analysis")
    st.caption("Get insights on your branch performance")
else:
    st.title("Smart Branch Assistant")
    st.caption("Start a new analysis by asking a question")

# Load messages for current session if not already loaded
if st.session_state.current_session_id and (
    "messages" not in st.session_state or not st.session_state.messages
):
    messages = get_session_messages(st.session_state.current_session_id)
    st.session_state.messages = [
        {"role": role, "content": content, "timestamp": timestamp}
        for _, role, content, timestamp in messages
    ]

# Display chat messages
if not st.session_state.messages:
    # Welcome message
    st.markdown(
        """
    <div class="welcome-message">
        <div class="welcome-title">Smart Branch Assistant</div>
        <div class="welcome-subtitle">
            Hi, I'm your Smart Branch Assistant and I'm ready to help your insight generating analysis based on your company's report.
            Here's some prompt template that you can use:
        </div>
    </div>
    """,
        unsafe_allow_html=True,
    )

    # Suggested questions
    col1, col2 = st.columns(2)

    suggestions = [
        "How many conversion from SPK to DO on August 2025?",
        "Show me the revenue sales for Traga on August 2025",
        "How much salesman productivity on August 2025?",
        "Show me the Traga sales revenue on July 2025",
    ]

    for i, suggestion in enumerate(suggestions):
        with col1 if i % 2 == 0 else col2:
            if st.button(suggestion, key=f"suggestion_{i}", use_container_width=True):
                # Set this as the user input
                st.session_state.user_input = suggestion
                st.rerun()

else:
    # Display existing messages
    for idx, msg in enumerate(st.session_state.messages):
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            st.caption(format_timestamp(msg["timestamp"]))

# Chat input
if prompt := st.chat_input("Type your question here", key="chat_input"):
    # Create session on first message if it doesn't exist
    if not st.session_state.current_session_id:
        # Create session with first question as title
        title = prompt[:50] + "..." if len(prompt) > 50 else prompt
        st.session_state.current_session_id = create_session(title)
        st.session_state.first_message = False

    # Handle user input
    timestamp = datetime.now().isoformat()

    # Add user message to session state
    st.session_state.messages.append(
        {"role": "user", "content": prompt, "timestamp": timestamp}
    )

    # Save to database
    add_message(st.session_state.current_session_id, "user", prompt)

    # Update session title if this is the first message (for existing sessions)
    if st.session_state.first_message:
        # Use first 50 chars of the question as title
        title = prompt[:50] + "..." if len(prompt) > 50 else prompt
        update_session_title(st.session_state.current_session_id, title)
        st.session_state.first_message = False

    # Display user message
    with st.chat_message("user"):
        st.markdown(prompt)
        st.caption(format_timestamp(timestamp))

    # Display assistant response with spinner
    with st.chat_message("assistant"):
        with st.spinner("Analyzing..."):
            # Prepare conversation history (exclude current prompt)
            conversation_history = [
                {"role": msg["role"], "content": msg["content"]}
                for msg in st.session_state.messages[:-1]  # Exclude the current user message we just added
            ]
            
            # Call backend with session context
            response = run_excel_analysis(
                "data-simplified.xlsx", 
                prompt,
                session_id=st.session_state.current_session_id,
                conversation_history=conversation_history
            )

        # Display response
        st.markdown(response)

        # Add timestamp
        response_timestamp = datetime.now().isoformat()
        st.caption(format_timestamp(response_timestamp))

        # Save assistant message
        st.session_state.messages.append(
            {"role": "assistant", "content": response, "timestamp": response_timestamp}
        )
        add_message(st.session_state.current_session_id, "assistant", response)

    # Rerun to update the UI
    st.rerun()

# Handle suggestion clicks
if "user_input" in st.session_state and st.session_state.user_input:
    prompt = st.session_state.user_input
    st.session_state.user_input = None  # Clear it

    # Create session on first message if it doesn't exist
    if not st.session_state.current_session_id:
        title = prompt[:50] + "..." if len(prompt) > 50 else prompt
        st.session_state.current_session_id = create_session(title)
        st.session_state.first_message = False

    timestamp = datetime.now().isoformat()

    # Add user message
    st.session_state.messages.append(
        {"role": "user", "content": prompt, "timestamp": timestamp}
    )
    add_message(st.session_state.current_session_id, "user", prompt)

    # Update title if first message (for existing sessions)
    if st.session_state.first_message:
        title = prompt[:50] + "..." if len(prompt) > 50 else prompt
        update_session_title(st.session_state.current_session_id, title)
        st.session_state.first_message = False

    # Get response
    with st.spinner("Analyzing..."):
        response = run_excel_analysis("data-simplified.xlsx", prompt)

    # Add assistant message
    response_timestamp = datetime.now().isoformat()
    st.session_state.messages.append(
        {"role": "assistant", "content": response, "timestamp": response_timestamp}
    )
    add_message(st.session_state.current_session_id, "assistant", response)

    st.rerun()

st.markdown("</div>", unsafe_allow_html=True)
