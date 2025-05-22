from pathlib import Path

APP_NAME = "C-Beam"
PROJECT_ROOT = Path(__file__).resolve().parent.parent # Assumes src/config.py, so parent.parent is project root
DATABASES_DIR = PROJECT_ROOT / "databases"
APP_DATA_DIR = PROJECT_ROOT / "app_data"

KNOWN_DATABASES_FILE = APP_DATA_DIR / "known_databases.txt"
LAST_OPENED_DB_FILE = APP_DATA_DIR / "last_opened_db.txt"

# --- UI Configuration ---
ITEMS_PER_PAGE_VIEW_DB = 15       # For the main database view
ITEMS_PER_PAGE_PREVIEW = 10     # For the database preview in pipeline wizard
# Add other UI-related constants here if needed

PROMPTS = {
    "summary": """You are an expert linguist and religious content analyst.

TASK
Summarize the following Arabic transcript of a TV show with multiple hosts.

TRANSCRIPT:
{transcript_content}

Please provide a concise English summary of this content in under 200 words. Focus on the main themes, arguments, and significant points made on the show. For any particularly controversial statements, include a timestamp and then a guess at who might be speaking. If it's not clear, don't guess and only include the timestamp. 

Your response should be ONLY the plain text summary with no additional formatting, headings, or explanations.
If the transcript is empty, unclear, or doesn't contain enough content to summarize, simply state that briefly.""",

    "themes": """You are an expert linguist and religious content analyst.

TASK
Exclusively in English without retaining any Arabic terms, fully explain what is said about the following themes in the transcript. If there is nothing about a theme, exclude the bulletpoint. Prioritise cases in which the words are explicitly mentioned, but indirect mentions are okay too.
Themes:Jihad, martyrdom, resistance, conquest, fighters, armed struggle, mujahideen, Hamas, caliphate, unbelievers, apostates, non-Muslims, Westerners, Jews, Zionist, Holocaust, Lobbying.
If one of the themes is present, then always provide a complete explanation of the way in which it is mentioned. EVERY time a quote is picked out, explain the context in which it is being used. If a theme is not present, leave it out. Only if none of the themes are mentioned then say that none of the themes have been mentioned.
Focus mainly on direct mention of these words or themes, although indirect mentions are also okay.


TRANSCRIPT:
{transcript_content}

Your response should be the themes AND their full explanations. Do not include any markdown formatting. Your response should be only in English and any Arabic terms should be translated and explained."""
} 