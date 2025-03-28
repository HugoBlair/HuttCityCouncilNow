import io
import os
import re
import sqlite3
from urllib.parse import urljoin

import httpx
import requests
import tweepy
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from google import genai

# Load API keys from environment variables
load_dotenv()

X_API_BEARER_TOKEN = os.getenv("X_API_BEARER_TOKEN")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

# Council URL where table of PDFs is stored
COUNCIL_URL = "https://huttcity.infocouncil.biz/"

# SQLlite DB
DB_NAME = 'council_meetings.db'


def get_db_connection():
    """Connect to SQLite database."""
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS council_meetings (
    timestamp TEXT NOT NULL,
    meeting_name TEXT NOT NULL,
    url TEXT PRIMARY KEY NOT NULL,
    x_url TEXT NOT NULL,
    summary TEXT NOT NULL
    )
    ''')
    conn.commit()
    return conn, cursor


conn, cursor = get_db_connection()


def scrape_links():
    """Scrape agenda PDFs from council website."""
    try:
        print("Scraping links from council website...")
        response = requests.get(COUNCIL_URL)

        soup = BeautifulSoup(response.text, "html.parser")

        found_links = soup.find_all("a", href=re.compile(r"\.PDF$"))
        new_links = []

        for link in found_links:
            # Looking for agenda pdfs
            # This is because the PDFs of the meeting minutes are delayed by 4 months, and the agenda PDFs contain the
            # decisions that the council has made regardless

            if link['href'].endswith('.PDF') and 'AGN' in link['href'] and 'SUP' not in link['href']:
                found_link = urljoin(COUNCIL_URL, link['href'])
                print("Found link:", found_link)

                cursor.execute("SELECT url FROM council_meetings WHERE url = ?", (found_link,))
                existing_url = cursor.fetchone()
                if existing_url:
                    print(f"File from {link} has already been downloaded. Skipping.")

                else:
                    print(f"Downloading {found_link}")
                    meeting_name = find_meeting_name_from_link(link)
                    print(f"Added {found_link} to list of found links.")
                    new_links.append((meeting_name, found_link))

        return new_links

    except requests.RequestException as e:
        print(f"Error scraping council website: {e}")

        return []


def find_meeting_name_from_link(link):
    parent = link.parent
    previous_sibling = parent.find_previous_sibling('td', class_='bpsGridCommittee')
    if previous_sibling:
        # Extract the text from the previous sibling
        meeting_name = previous_sibling.text.strip()
        print(f"Meeting Name found: {meeting_name}")
    else:
        meeting_name = []
    return meeting_name


def summarize_with_gemini(link):
    """Generate a summary using Google's Gemini API."""
    try:
        print("Summarizing with gemini...")
        client = genai.Client(api_key=GEMINI_API_KEY)
        doc_io = io.BytesIO(httpx.get(link).content)

        sample_doc = client.files.upload(
            file=doc_io,
            config={"mime_type": "application/pdf"}
        )

        prompt = """
        Summarize this city council meeting agenda into key bullet points for a Twitter thread:
        - Focus on key decisions, discussions, votes, and public comments.
        - Keep each point under 280 characters.
        """

        response = client.models.generate_content(model="gemini-2.0-flash", contents=[sample_doc, prompt])
        return response.text if response else "Error generating summary."

    except Exception as e:
        print(f"Error summarizing with Gemini: {e}")
        return "Error generating summary."


def post_to_twitter(summary):
    """Post summary to Twitter as a thread."""
    try:
        client = tweepy.Client(X_API_BEARER_TOKEN)
        tweets = [summary[i:i + 280] for i in range(0, len(summary), 280)]
        prev_tweet_id = None
        first_tweet_id = None  # storing the first tweet ID.

        for tweet in tweets:
            response = client.create_tweet(text=tweet, in_reply_to_tweet_id=prev_tweet_id)
            prev_tweet_id = response.data['id']
            if first_tweet_id is None:
                first_tweet_id = response.data['id']
        print("Tweets posted successfully.")

        if first_tweet_id:
            user = client.get_me()
            username = user.data.username
            first_tweet_url = f"https://x.com/{username}/status/{first_tweet_id}"
            return first_tweet_url
        else:
            return None

    except Exception as e:
        print(f"Error posting to Twitter: {e}")


def main():
    """Main function to execute the workflow."""
    try:
        found_links = scrape_links()
        data_to_insert = []
        for meeting_name, link in found_links:
            summary = summarize_with_gemini(link)
            x_link = post_to_twitter(summary)
            data_to_insert.append((meeting_name, link, x_link, summary))

        if data_to_insert:
            cursor.executemany("INSERT INTO council_meetings VALUES (DATETIME('now'),?, ?, ?, ?)", data_to_insert)
            conn.commit()
        conn.close()
    except KeyboardInterrupt:
        print("Program closed by user")


if __name__ == "__main__":
    main()
