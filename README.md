*Hutt Council Meeting Scraper*

**What is this?**

This is the code that powers the @HuttCouncilNow X (Twitter) account. The bot automatically finds, downloads, summarizes, and tweets about Hutt City Council meeting agendas and decisions.

**What does it do?**

- Scrapes the Hutt City Council website for new meeting agenda PDFs
- Downloads these PDFs and checks if they've already been processed
- Summarizes the content using Google's Gemini AI
- Posts the summaries as tweet threads
- Tracks all processed documents in a SQLite database

**Why is it useful?**

Most people don't have time to read through lengthy council documents, but they still want to know what's happening in their local government. This bot makes local democracy more accessible by:

- Distilling lengthy council documents into concise, readable summaries
- Highlighting key decisions that affect the community
- Automatically sharing this information where people already spend time (X/Twitter)

**Tech stack**

- Python for the core functionality
- BeautifulSoup for web scraping
- Google Gemini API for AI-powered document summarization
- Tweepy for posting to X/Twitter
- SQLite for tracking processed documents
- Google Cloud e2 instance for hosting
- Cron jobs to trigger the script multiple times daily

**How it runs**
The script is hosted on a Google Cloud Platform e2 instance and runs multiple times per day via cron jobs to check for and process new council documents.
No human intervention required - it's a fully automated civic information pipeline!

***Follow @HuttCouncilNow to stay informed about important decisions from Hutt City Council.***
