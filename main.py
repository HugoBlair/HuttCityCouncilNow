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
    timestamp TEXT,
    url TEXT PRIMARY KEY
    )
    ''')
    conn.commit()
    return conn, cursor


def scrape_links():
    """Scrape agenda PDFs from council website."""
    conn, cursor = get_db_connection()
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

            if link['href'].endswith('.pdf') and 'AGN' in link['href']:
                found_link = urljoin(COUNCIL_URL, link['href'])
                cursor.execute("SELECT url FROM council_meetings WHERE url = ?", (found_link,))
                existing_url = cursor.fetchone()
                if existing_url:
                    print(f"File from {link} has already been downloaded. Skipping.")

                else:
                    new_links.append(found_link)
                    cursor.execute(f"INSERT INTO council_meetings VALUES (DATETIME('now'),?)", found_link)
                    conn.commit()
                    print(f"Added {found_link} to database.")

        conn.close()
        return new_links

    except requests.RequestException as e:
        print(f"Error scraping council website: {e}")
        conn.close()
        return []


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

        for tweet in tweets:
            response = client.create_tweet(text=tweet, in_reply_to_tweet_id=prev_tweet_id)
            prev_tweet_id = response.data['id']

        print("Tweets posted successfully.")
    except Exception as e:
        print(f"Error posting to Twitter: {e}")


def main():
    """Main function to execute the workflow."""
    found_links = scrape_links()
    for link in found_links:
        summary = summarize_with_gemini(link)
        post_to_twitter(summary)


if __name__ == "__main__":
    main()
