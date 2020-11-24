#!/usr/bin/python3

from bs4 import BeautifulSoup
from datetime import date
import feedparser
import html5lib
import json
import os
from pprint import pprint
import re
import requests
import time

FEEDS = {
    "nyt_health": "https://rss.nytimes.com/services/xml/rss/nyt/Health.xml",
    "nyt_science": "https://rss.nytimes.com/services/xml/rss/nyt/Science.xml",
    "nyt_travel": "https://rss.nytimes.com/services/xml/rss/nyt/Travel.xml",
    "wired_rss": "https://www.wired.com/feed/rss",
    "mit_technology_review_rss": "https://cdn.technologyreview.com/stories.rss",
}

FEEDS_SCRAPE_TAG = {
    "nyt_health": "StoryBodyCompanionColumn",
    "nyt_science": "StoryBodyCompanionColumn",
    "nyt_travel": "StoryBodyCompanionColumn",
    "wired_rss": "article__chunks",
    "mit_technology_review_rss": "storyContent",
}


def _scrape_article_text(
    feed_dir: str, feed_title: str, feed_scrape_tag: str, page_title: str, page_url: str
) -> str:
    # Get text of article
    article_html = requests.get(page_url)
    article_content = article_html.content
    soup_article = BeautifulSoup(article_content, "html5lib")
    article_bodies = soup_article.find_all("div", class_=re.compile(feed_scrape_tag))
    if not article_bodies:
        print(f"Could not find article body for {page_title} at {page_url}")
        """
        with open(f"{page_title}.txt", "w") as f:
            f.write(soup_article.prettify())
        """
        return ""

    # Create feed dir and filename
    feed_domain_dir = os.path.join(feed_dir, feed_title)
    if not os.path.exists(feed_domain_dir):
        os.makedirs(feed_domain_dir)
    page_title_filename = f"{page_title.replace(' ', '_')}.txt"
    article_text_path = os.path.join(feed_domain_dir, page_title_filename)

    # Write article text into these files
    with open(article_text_path, "w+") as article_text_file:
        for article_body in article_bodies:
            body_text = article_body.find_all("p")
            for paragraph in body_text:
                paragraph_text = paragraph.get_text()
                article_text_file.write(f"{paragraph_text}\n")

    return ""


def _summarize_news_articles(feed_dir: str) -> None:
    """TODO: Summarize news articles using Hugging face transformers"""
    pass


def parse_and_upload_rss_feed_data(feed_data_filename: str):
    """
    TODO: Filter and parse RSS feed data for coronavirus related articles
    """
    curr_date_str = date.today().strftime("%m-%d-%Y")
    feed_dir = f"rss_feeds_{curr_date_str}"
    if not os.path.exists(feed_dir):
        os.makedirs(feed_dir)
    feed_data_path = os.path.join(feed_dir, feed_data_filename)

    with open(feed_data_path, "w+") as feed_data_file:
        for feed, rss_url in FEEDS.items():
            rss_parsed = feedparser.parse(rss_url)
            feed_data_file.write(f"Feed: {feed}\n")
            feed_data_file.write(json.dumps(rss_parsed, indent=4))

            feed_scrape_tag = FEEDS_SCRAPE_TAG[feed]
            feed_metadata = rss_parsed["feed"]
            feed_title = feed_metadata["title"]
            feed_entries = rss_parsed["entries"]
            if not feed_entries:
                continue

            for feed_entry in feed_entries:
                article_title = feed_entry["title"]
                article_url = feed_entry["link"]
                _scrape_article_text(
                    feed_dir, feed_title, feed_scrape_tag, article_title, article_url
                )

    _summarize_news_articles(feed_dir)


def main():
    start_time = time.time()
    parse_and_upload_rss_feed_data(f"feed_data.txt")
    end_time = time.time()
    print(f"Execution time (sync): {end_time - start_time}")


if __name__ == "__main__":
    main()
