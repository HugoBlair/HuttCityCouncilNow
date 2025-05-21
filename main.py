import io
import logging
import os
import re
import sqlite3
import traceback
from urllib.parse import urljoin

import httpx
import requests
import tweepy
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from google import genai

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE_PATH = os.path.join(BASE_DIR, 'council_scraper.log')
DB_PATH = os.path.join(BASE_DIR, 'council_meetings.db')

# Set up logging
logger = logging.getLogger('council_scraper')
logger.setLevel(logging.INFO)

# Create console handler and set level
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)

# Create file handler and set level
file_handler = logging.FileHandler(LOG_FILE_PATH, "a", "utf-8")
file_handler.setLevel(logging.INFO)

# Create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

# Add handlers to logger
logger.addHandler(console_handler)
logger.addHandler(file_handler)

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


def get_db_connection():
    """Connect to SQLite database."""
    conn = sqlite3.connect(DB_PATH)
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

        logger.info("Scraping links from council website...")
        response = requests.get(COUNCIL_URL)

        soup = BeautifulSoup(response.text, "html.parser")

        # Change limit to adjust how far the program should look back through the agenda files
        found_links = soup.find_all("a", href=re.compile(r"\.PDF$"), limit=1)
        new_links = []

        for link in found_links:
            # Looking for agenda pdfs
            # This is because the PDFs of the meeting minutes are delayed by 4 months, and the agenda PDFs contain the
            # decisions that the council has made regardless

            if link['href'].endswith('.PDF') and 'AGN' in link['href'] and 'SUP' not in link['href']:
                found_link = urljoin(COUNCIL_URL, link['href'])
                logger.info("Found link: %s", found_link)

                cursor.execute("SELECT url FROM council_meetings WHERE url = ?", (found_link,))
                existing_url = cursor.fetchone()
                if existing_url:
                    logger.info("File from %s has already been downloaded. Skipping.", link)

                else:
                    logger.info("Downloading %s", found_link)
                    committee_name = find_committee_name_from_link(link)
                    logger.info("Added %s to list of found links.", found_link)
                    new_links.append((committee_name, found_link))
                    with open("downloaded.pdf", "wb") as f:
                        f.write(httpx.get(found_link).content)

        return new_links

    except requests.RequestException as e:
        logger.error("Error scraping council website: %s", e)

        return []


def find_committee_name_from_link(link):
    parent = link.parent
    committee_td = parent.find_previous_sibling('td', class_='bpsGridCommittee')
    br_tag = committee_td.find('br')
    if committee_td and br_tag:
        # Extract the text from the previous sibling
        committee_name = br_tag.previous_sibling.strip()
        logger.info(f"Meeting Name found: {committee_name}")
    elif committee_td:
        committee_name = committee_td.text.strip()
        logger.info(f"Meeting Name found: {committee_name}")
    else:
        committee_name = "Unknown Committee"
        logger.info("Meeting name not found")

    return committee_name


def summarize_with_gemini(committee_name, link):
    """Generate a summary using Google's Gemini API."""
    try:
        logger.info("Summarizing with gemini...")
        client = genai.Client(api_key=GEMINI_API_KEY)
        doc_io = io.BytesIO(httpx.get(link, follow_redirects=True, timeout=120).content)
        logger.debug(doc_io.getvalue())

        if doc_io.getbuffer().nbytes < 100:  # Arbitrary small size check
            logger.warning(f"Warning: File size is very small {doc_io.getbuffer().nbytes} bytes). May not be a valid "
                           f"PDF.")

        sample_doc = client.files.upload(
            file=doc_io,
            config=dict(
                mime_type='application/pdf')
        )

        prompt = f"""
        Summarize this city council meeting into short paragraphs.
        - Focus on key decisions, discussions, votes, and public comments.
        - Only include things that are vitally important and are interesting to an audience. 
        - Do not include boring information such as members present and public comment.
        - Focus on delivering information that is significant to the city.
        - Include important specifics and details
        - Do not comment on opening or closing formalities
        - Do not put anything in bold.
        
        Begin your response with "The {committee_name} met to discuss " followed by the main subject of the meeting. 
        Follow this with a sentence about the main subject of the meeting.
        Add a small amount of popular hashtags into the tweet but only if relevant.
                
        Use natural formatting like this:
        • Topic A
        • Topic B
        • Topic C
        
        Ensure you begin each point on a new line.

        """

        response = client.models.generate_content(model="gemini-2.0-flash", contents=[sample_doc, prompt])
        if response:
            logger.info("Successfully generated summary")

            return response.text
        else:
            logger.error("Error generating summary.")

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP Error downloading PDF from {link}: {e.response.status_code} - {e.response.text}")
    except httpx.RequestError as e:
        logger.error(f"Request Error downloading PDF from {link}: {traceback.format_exc()}")
    except Exception as e:
        # Catch the specific Gemini API error if possible, otherwise generic
        logger.error(f"Error summarizing with Gemini: {e}")


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

        prev_tweet_id = None
        first_tweet_id = None  # storing the first tweet ID.
        tweet_chunks = generate_tweet(summary.splitlines())
        tweet_chunks_len = len(tweet_chunks)
        logger.info("Chunked tweets:")
        logger.info(tweet_chunks)

        for x, tweet in enumerate(tweet_chunks):
            tweet = tweet + f" ({x + 1}/{tweet_chunks_len})"
            response = client.create_tweet(text=tweet, in_reply_to_tweet_id=prev_tweet_id)
            logger.info(f"Posted tweet to Twitter: {tweet}")
            prev_tweet_id = response.data['id']
            if first_tweet_id is None:
                first_tweet_id = response.data['id']
        logger.info("Tweets posted successfully.")

        if first_tweet_id:
            user = client.get_me()
            username = user.data.username
            first_tweet_url = f"https://x.com/{username}/status/{first_tweet_id}"
            return first_tweet_url
        else:
            return None

    except Exception as e:
        logger.error("Error posting to Twitter. It is likely that the ratelimit has been hit: %s\n",
                     traceback.format_exc())


def generate_tweet(lines):
    tweet_length_limit = 270
    tweets = []
    tweet = ""

    for line in lines:
        len_line = len(line)

        if len_line >= tweet_length_limit:
            tweets.append(tweet)
            tweet = ""

            new_line_1 = ""
            new_line_2 = ""

            length = 0
            for word in line.split():
                length += (len(word) + 1)

                if length < tweet_length_limit:
                    new_line_1 += word + " "
                if length >= tweet_length_limit:
                    new_line_2 += word + " "

            tweets.append(new_line_1)
            tweets.append(new_line_2)
        else:
            len_tweet = len(tweet)

            if len_tweet >= tweet_length_limit:
                tweets.append(tweet)
                tweet = line
            elif len_tweet + len_line >= tweet_length_limit:
                tweets.append(tweet)
                tweet = line
            else:
                tweet += line + " "

    if tweet != "":
        tweets.append(tweet)
    return tweets


def main():
    """Main function to execute the workflow."""
    try:
        found_links = scrape_links()
        data_to_insert = []
        for committee_name, link in found_links:
            summary = summarize_with_gemini(committee_name, link)
            x_link = post_to_twitter(summary)
            if committee_name and link and x_link and summary:
                data_to_insert.append((committee_name, link, x_link, summary))

        if data_to_insert:
            cursor.executemany("INSERT INTO council_meetings VALUES (DATETIME('now'),?, ?, ?, ?)", data_to_insert)
            logger.info("Successfully inserted data")
            conn.commit()
        conn.close()
    except KeyboardInterrupt:
        logger.info("Program has been closed by the user")
    except sqlite3.IntegrityError as e:
        logger.error("Sqllite Not Null clause violated: %s", e)


if __name__ == "__main__":
    main()
