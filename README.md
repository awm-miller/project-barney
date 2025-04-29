# Transcription and Analysis Pipeline

This project provides a pipeline for searching YouTube channels, downloading relevant videos based on dates, transcribing the audio using Google Cloud Speech-to-Text, analyzing the transcripts using Google's Gemini API, and generating an HTML report with the findings.

## Features

*   **Channel Search:** Finds YouTube channels based on a list of institution names (`unique_institutions.csv`).
*   **Video Download:** Downloads videos closest to specified target dates (`target_dates.txt`) from found channels using `yt-dlp`.
*   **Audio Transcription:** Extracts audio using `ffmpeg` and transcribes it using Google Cloud Speech-to-Text (handles large files via GCS).
*   **AI Analysis:** Analyzes transcripts for specific themes or content using Google's Gemini Pro API.
*   **Reporting:** Generates an HTML report summarizing the analysis and providing links to transcripts and analysis details.
*   **Data Management:** Uses a SQLite database (path configured via `.env`) to track video processing status.

## Prerequisites

1.  **Python 3.x:** Ensure a recent version of Python 3 is installed ([python.org](https://www.python.org/)).
2.  **Pip:** Python's package installer (usually comes with Python).
3.  **ffmpeg:** Required for audio extraction from videos.
    *   Download from [ffmpeg.org](https://ffmpeg.org/download.html).
    *   Install it and ensure the `ffmpeg` executable is in your system's PATH environment variable so the scripts can find it. You can test this by opening a terminal/command prompt and simply typing `ffmpeg`.
4.  **Google Cloud Account & APIs:**
    *   You need a Google Cloud Platform (GCP) account ([cloud.google.com](https://cloud.google.com/)).
    *   Create a new GCP Project or use an existing one.
    *   Enable the following APIs within your GCP project:
        *   **YouTube Data API v3** (for searching channels/videos)
        *   **Cloud Speech-to-Text API** (for transcription)
        *   **Cloud Storage API** (for temporarily storing large audio files for transcription)
        *   **Generative Language API** (for accessing the Gemini model for analysis)
    *   Create a **Service Account**:
        *   Go to "IAM & Admin" -> "Service Accounts" in your GCP console.
        *   Create a new service account.
        *   Grant it necessary roles. Essential roles include:
            *   `Storage Object Admin` (to manage files in the GCS bucket)
            *   `Cloud Speech Service Agent` (or similar role granting permission to use the Speech-to-Text API)
            *   *Consider adding other roles if needed based on GCP setup.*
        *   Create a **JSON key** for this service account and download it securely. You will need the path to this file.
    *   Create a **Cloud Storage Bucket**:
        *   Go to "Cloud Storage" -> "Buckets" in your GCP console.
        *   Create a new bucket. Choose a unique name, region, and default storage class. You will need this bucket name.
5.  **API Keys:**
    *   **YouTube API Key:** Create an API Key credential within your GCP project's "APIs & Services" -> "Credentials" section. Restrict its usage if necessary (e.g., to specific APIs or IP addresses).
    *   **Gemini API Key:** Generate an API key for the Generative Language API via Google AI Studio ([aistudio.google.com/app/apikey](https://aistudio.google.com/app/apikey)).
6.  **`yt-dlp` Cookies (Optional but Recommended):**
    *   Some YouTube channels or videos might require you to be logged in (e.g., age-restricted content).
    *   To allow `yt-dlp` (used by the download script) to access these, you can export your browser's YouTube cookies.
    *   Use a browser extension designed for this, such as "Get cookies.txt LOCALLY" (available for Chrome/Firefox).
    *   Export the cookies for the `youtube.com` domain into a file named `cookies.txt`.

## Detailed Setup Instructions

Follow these steps carefully to set up the project environment:

1.  **Clone the Repository:**
    *   Open your terminal or command prompt.
    *   Navigate to the directory where you want to store the project.
    *   Run:
        ```bash
        git clone <repository_url> # Replace <repository_url> with the actual URL
        cd transcription-project   # Navigate into the cloned directory
        ```

2.  **Create and Activate Virtual Environment:** (Highly Recommended)
    *   This isolates project dependencies.
    *   Run:
        ```bash
        python -m venv .venv
        ```
    *   Activate the environment:
        *   **Windows (Command Prompt):** `.venv\\Scripts\\activate.bat`
        *   **Windows (PowerShell):** `.venv\\Scripts\\Activate.ps1` (You might need to adjust execution policy: `Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process`)
        *   **macOS/Linux:** `source .venv/bin/activate`
    *   Your terminal prompt should now show `(.venv)` at the beginning.

3.  **Install Python Dependencies:**
    *   With the virtual environment activated, run:
        ```bash
        pip install -r requirements.txt
        ```

4.  **Configure Environment Variables (`.env` file):**
    *   Create a new file named `.env` in the root directory of the project (the same directory as `requirements.txt`).
    *   Open the `.env` file in a text editor.
    *   Copy and paste the following template, **replacing all placeholder values** with your actual keys, paths, and bucket name obtained in the "Prerequisites" step:
        ```dotenv
        # --- Required API Keys ---
        YOUTUBE_API_KEY="YOUR_YOUTUBE_API_KEY"
        # Use the full path to the downloaded service account JSON key file
        GOOGLE_APPLICATION_CREDENTIALS="C:/path/to/your/downloaded/service-account-key.json" # Example path
        GEMINI_API_KEY="YOUR_GEMINI_API_KEY"

        # --- Required Paths & Names ---
        # Use absolute paths to avoid issues when running scripts
        # Directory where videos will be downloaded by download_videos.py
        DOWNLOAD_DIR="C:/path/to/your/project/video_downloads" # Example path

        # Directory where transcript .txt files will be saved by transcribe_videos.py
        TRANSCRIPTS_DIR="C:/path/to/your/project/transcripts" # Example path

        # Directory where analysis .json files will be saved by analysis_stack.py
        ANALYSIS_DIR="C:/path/to/your/project/analysis_output" # Example path

        # Full path to the SQLite database file (e.g., transcription_data.db)
        # This file will be created by transcribe_videos.py if it doesn't exist
        DATABASE_PATH="C:/path/to/your/project/data/transcription_data.db" # Example path

        # Name of your Google Cloud Storage bucket (created in Prerequisites)
        GCS_BUCKET_NAME="your-unique-gcs-bucket-name"

        # --- Optional Input File Paths (Defaults are usually fine) ---
        # INPUT_INSTITUTIONS_FILE="unique_institutions.csv"
        # INPUT_DATES_FILE="target_dates.txt"
        ```
    *   **Important Notes:**
        *   Use **absolute paths** for `GOOGLE_APPLICATION_CREDENTIALS`, `DOWNLOAD_DIR`, `TRANSCRIPTS_DIR`, `ANALYSIS_DIR`, and `DATABASE_PATH`. Replace example paths with your real ones.
        *   On Windows, use forward slashes (`/`) or escaped backslashes (`\\\\`) in paths within the `.env` file (e.g., `C:/Users/You/Project` or `C:\\\\Users\\\\You\\\\Project`).
        *   The directories for downloads, transcripts, and analysis do *not* need to exist beforehand; the scripts will attempt to create them. The directory for the database *will* be created if necessary.
        *   **SECURITY:** The `.env` file contains sensitive API keys. Ensure it is **added to your `.gitignore` file** to prevent accidentally committing it to version control. The provided `.gitignore` should already include `.env`.

5.  **Prepare Input Data Files:**
    *   **Institutions List:** Create a file named `unique_institutions.csv` in the project root (or the path specified by `INPUT_INSTITUTIONS_FILE` in `.env`). Add the names of the institutions (e.g., mosques, organizations) you want to search for on YouTube, one name per line.
    *   **Target Dates:** Create a file named `target_dates.txt` in the project root (or the path specified by `INPUT_DATES_FILE` in `.env`). Add the dates you are interested in, one date per line, in `YYYY-MM-DD` format. The download script will look for videos published near these dates.
    *   **Cookies (Optional):** If you created a `cookies.txt` file (see Prerequisites step 7), place it in the project root directory.

## Running the Pipeline (Script Order)

The project consists of several Python scripts designed to be run in a specific sequence. Each script performs a distinct step in the pipeline, often using the output of the previous step. Ensure your `.env` file is correctly configured before running.

**Run the scripts from the project's root directory in your activated virtual environment.**

**Step 1: Search for YouTube Channels**

*   **Script:** `search_channels.py`
*   **Purpose:** Takes the list of institution names and uses the YouTube API to find potential corresponding YouTube channel IDs.
*   **Command:**
    ```bash
    python search_channels.py --output search_results.csv
    ```
    *   *(Optional: Use `--institutions path/to/your_list.csv` to specify a different input file)*
*   **Inputs:** `unique_institutions.csv` (or specified file), `YOUTUBE_API_KEY` (from `.env`)
*   **Outputs:** `search_results.csv` (contains institution names, found channel IDs, and channel titles)

**Step 2: Download Videos**

*   **Script:** `download_videos.py`
*   **Purpose:** Reads the `search_results.csv`, finds videos on the identified channels published near the dates in `target_dates.txt`, and downloads the closest match for each channel using `yt-dlp`.
*   **Command:**
    ```bash
    python download_videos.py --search-csv search_results.csv --output download_results.csv
    ```
    *   *(Optional: Use `--dates path/to/dates.txt`, `--download-dir path/to/downloads` to override `.env` settings)*
*   **Inputs:** `search_results.csv`, `target_dates.txt` (or specified file), `DOWNLOAD_DIR` (from `.env`), `YOUTUBE_API_KEY` (from `.env`), `cookies.txt` (optional)
*   **Outputs:** Video files saved to `DOWNLOAD_DIR`, `download_results.csv` (metadata about downloaded videos), `missing_videos_*.txt` (if videos couldn't be found/downloaded)

**Step 3: Transcribe Videos**

*   **Script:** `transcribe_videos.py`
*   **Purpose:** Scans the download directory for video files, extracts audio using `ffmpeg`, uploads audio to Google Cloud Storage (necessary for long files), transcribes the audio using Google Cloud Speech-to-Text, saves the text transcripts, and updates the database. Initializes the database if it doesn't exist.
*   **Command:**
    ```bash
    python transcribe_videos.py
    ```
    *   *(Optional: Use `--video-dir`, `--transcript-dir`, `--gcs-bucket`, `--db-path` to override `.env` settings)*
*   **Inputs:** Video files in `DOWNLOAD_DIR` (from `.env`), `TRANSCRIPTS_DIR` (from `.env`), `GCS_BUCKET_NAME` (from `.env`), `DATABASE_PATH` (from `.env`), `GOOGLE_APPLICATION_CREDENTIALS` (from `.env`), `ffmpeg` (must be in PATH)
*   **Outputs:** Transcript `.txt` files saved to `TRANSCRIPTS_DIR`, audio files temporarily uploaded/deleted from `GCS_BUCKET_NAME`, updates status in `DATABASE_PATH`.

**Step 4: Analyze Transcripts**

*   **Script:** `analysis_stack.py`
*   **Purpose:** Queries the database for completed transcripts, sends the transcript text to the Gemini API for analysis based on the configured prompt (looking for specific types of content), saves the JSON analysis results, and updates the database.
*   **Command:**
    ```bash
    python analysis_stack.py
    ```
    *   *(Optional: Use `--db-path`, `--analysis-dir` to override `.env` settings)*
*   **Inputs:** `DATABASE_PATH` (from `.env`), `ANALYSIS_DIR` (from `.env`), `GEMINI_API_KEY` (from `.env`), transcript files (paths read from database)
*   **Outputs:** Analysis `.json` files saved to `ANALYSIS_DIR`, updates status in `DATABASE_PATH`.

**Step 5: Generate Report**

*   **Script:** `generate_report.py`
*   **Purpose:** Reads the database, transcripts, and analysis results to generate a consolidated HTML report and a ZIP archive containing the report and all associated data files.
*   **Command:**
    ```bash
    python generate_report.py --output-html report.html --output-zip report_archive.zip
    ```
    *   *(Optional: Use `--db-path`, `--analysis-dir`, `--transcripts-dir` to override `.env` settings)*
*   **Inputs:** `DATABASE_PATH` (from `.env`), `ANALYSIS_DIR` (from `.env`), `TRANSCRIPTS_DIR` (from `.env`), analysis files, transcript files.
*   **Outputs:** `report.html` (the viewable report), `report_archive.zip` (contains the HTML report, DB, transcripts, and analysis JSONs).

**Summary of Order:**

`search_channels.py` -> `download_videos.py` -> `transcribe_videos.py` -> `analysis_stack.py` -> `generate_report.py`

You must run them in this order for the pipeline to function correctly.

## Resetting Analysis

If you need to re-run the analysis step:

```bash
# Uses DATABASE_PATH and ANALYSIS_DIR from .env
python reset_analysis.py
```
This script resets the status in the database (`DATABASE_PATH`) and attempts to delete the `analysis_map.json` within `ANALYSIS_DIR`.

## Important Files & Configuration

*   **.env:** Stores API keys and **required** path configurations. **Keep this file secure and add it to `.gitignore`!**
*   **`requirements.txt`:** Lists Python dependencies.
*   **`unique_institutions.csv`:** Default input list of institutions (path configurable in `.env`).
*   **`target_dates.txt`:** Default input list of target dates (path configurable in `.env`).
*   **`cookies.txt`:** (Optional) YouTube cookies for `yt-dlp`.
*   **`*.log`:** Log files for each script.
*   **`transcription_data.db`:** SQLite database tracking processing state (path configured in `.env`).
*   **`ffmpeg`:** External tool dependency. Must be in PATH.
*   **Service Account JSON:** Google Cloud credentials file (path configured in `.env`).

## Troubleshooting

*   **`ValueError: ... not found in .env file`:** Ensure all required variables (API Keys, `DOWNLOAD_DIR`, `TRANSCRIPTS_DIR`, `ANALYSIS_DIR`, `DATABASE_PATH`, `GCS_BUCKET_NAME`) are correctly defined in your `.env` file.
*   **Quota Errors:** Check Google Cloud & YouTube API quotas.
*   **ffmpeg Not Found:** Ensure ffmpeg is installed and in your system PATH.
*   **File Not Found Errors:** Double-check paths in `.env`. Ensure the service account specified in `GOOGLE_APPLICATION_CREDENTIALS` exists and has permissions for GCS and Speech API.
*   **Authentication Errors:** Verify API keys and the Google Cloud Service Account setup.
*   **`yt-dlp` Issues:** Ensure `yt-dlp` is up-to-date (`pip install --upgrade yt-dlp`). Use `cookies.txt` if needed. 