import io
import math
import os
import re
import sqlite3
import textwrap
from urllib.parse import urljoin

import httpx
import requests
import tweepy
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from google import genai

# Load API keys from environment variables
load_dotenv()

X_API_CONSUMER_KEY = os.getenv("X_API_CONSUMER_KEY")
X_API_CONSUMER_KEY_SECRET = os.getenv("X_API_CONSUMER_KEY_SECRET")
X_API_ACCESS_TOKEN = os.getenv("X_API_ACCESS_TOKEN")
X_API_ACCESS_TOKEN_SECRET = os.getenv("X_API_ACCESS_TOKEN_SECRET")
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
    committee_name TEXT NOT NULL,
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

        found_links = soup.find_all("a", href=re.compile(r"\.PDF$"), limit=1)
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
                    committee_name = find_committee_name_from_link(link)
                    print(f"Added {found_link} to list of found links.")
                    new_links.append((committee_name, found_link))
                    with open("downloaded.pdf", "wb") as f:
                        f.write(httpx.get(found_link).content)

        return new_links

    except requests.RequestException as e:
        print(f"Error scraping council website: {e}")

        return []


def find_committee_name_from_link(link):
    parent = link.parent
    committee_td = parent.find_previous_sibling('td', class_='bpsGridCommittee')
    br_tag = committee_td.find('br')
    if committee_td and br_tag:
        # Extract the text from the previous sibling
        committee_name = br_tag.previous_sibling.strip()
        print(f"Meeting Name found: {committee_name}")
    elif committee_td:
        committee_name = committee_td.text.strip()
        print(f"Meeting Name found: {committee_name}")
    else:
        committee_name = "Unknown Committee"
        print("Meeting name not found")

    return committee_name


def summarize_with_gemini(committee_name, link):
    """Generate a summary using Google's Gemini API."""
    try:
        print("Summarizing with gemini...")
        client = genai.Client(api_key=GEMINI_API_KEY)
        doc_io = io.BytesIO(httpx.get(link, follow_redirects=True, timeout=30).content)

        if doc_io.getbuffer().nbytes < 100:  # Arbitrary small size check
            print(f"Warning: File size is very small ({doc_io.getbuffer().nbytes} bytes). May not be a valid PDF.")

        sample_doc = client.files.upload(
            file=doc_io,
            config=dict(
                mime_type='application/pdf')
        )

        prompt = f"""
        Summarize this city council meeting in short paragraphs.
        - Focus on key decisions, discussions, votes, and public comments.
        - Only include things that are vitally important and are interesting to an audience. 
        - Do not include boring information such as members present and public comment.
        - Focus on delivering information that is significant to the city.
        - Include important specifics and details

        Begin your response with "The {committee_name} met to discuss:"
        Every time you want a new line, you must type "/n" instead.

        """

        response = client.models.generate_content(model="gemini-2.0-flash", contents=[sample_doc, prompt])
        if response:
            print("Successfully generated summary")
            return response.text
        else:
            print("Error generating summary.")

    except httpx.HTTPStatusError as e:
        print(f"HTTP Error downloading PDF from {link}: {e.response.status_code} - {e.response.text}")
        return f"Error generating summary: Failed to download PDF ({e.response.status_code})."
    except httpx.RequestError as e:
        print(f"Request Error downloading PDF from {link}: {e}")
        return "Error generating summary: Network error during download."
    except Exception as e:
        # Catch the specific Gemini API error if possible, otherwise generic
        print(f"Error summarizing with Gemini: {e}")
        # Check if the error string contains the specific message
        return "Error generating summary."


def post_to_twitter(summary):
    """Post summary to Twitter as a thread."""
    try:
        client = tweepy.Client(
            X_API_BEARER_TOKEN,
            X_API_CONSUMER_KEY,
            X_API_CONSUMER_KEY_SECRET,
            X_API_ACCESS_TOKEN,
            X_API_ACCESS_TOKEN_SECRET
        )
        tweet_length = len(summary)
        tweets = []

        if tweet_length > 272:
            tweet_length_limit = tweet_length / 272

            tweet_chunk_length = tweet_length / math.ceil(tweet_length_limit)

            # chunk the tweet into individual pieces
            tweet_chunks = textwrap.wrap(summary, math.ceil(tweet_chunk_length), break_long_words=False)

            # iterate over the chunks
            for x, chunk in zip(range(len(tweet_chunks)), tweet_chunks):
                if x == 0:
                    tweets.append(f' {chunk} (1/{len(tweet_chunks)})')
                else:
                    tweets.append(f'{chunk} ({x + 1}/{len(tweet_chunks)})')

        prev_tweet_id = None
        first_tweet_id = None  # storing the first tweet ID.

        for tweet in tweets:
            response = client.create_tweet(text=tweet.replace("\\n", "\n"), in_reply_to_tweet_id=prev_tweet_id)
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
        for committee_name, link in found_links:
            summary = summarize_with_gemini(committee_name, link)
            x_link = post_to_twitter(summary)
            data_to_insert.append((committee_name, link, x_link, summary))

        if data_to_insert:
            cursor.executemany("INSERT INTO council_meetings VALUES (DATETIME('now'),?, ?, ?, ?)", data_to_insert)
            conn.commit()
        conn.close()
    except KeyboardInterrupt:
        print("Program has been closed by the user")


if __name__ == "__main__":
    main()
