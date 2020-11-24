#!/usr/bin/python3

from bs4 import BeautifulSoup
import feedparser
import html5lib
import json
import os
from pprint import pprint
import re
import requests

FEEDS = {
    "nyt_health": "https://rss.nytimes.com/services/xml/rss/nyt/Health.xml",
    "nyt_science": "https://rss.nytimes.com/services/xml/rss/nyt/Science.xml",
    "nyt_travel": "https://rss.nytimes.com/services/xml/rss/nyt/Travel.xml",
    "wsj_lifestyle": "https://feeds.a.dj.com/rss/RSSLifestyle.xml",
    "wsj_world_news": "https://feeds.a.dj.com/rss/RSSWorldNews.xml",
    "wired_rss": "https://www.wired.com/feed/rss",
    "mit_technology_review_rss": "https://cdn.technologyreview.com/stories.rss",
}

FEEDS_SCRAPE_TAG = {
    "nyt_health": "StoryBodyCompanionColumn",
    "nyt_science": "StoryBodyCompanionColumn",
    "nyt_travel": "StoryBodyCompanionColumn",
    "wsj_lifestyle": "article-content",
    "wsj_world_news": "article-content",
    "wired_rss": "article__chunks",
    "mit_technology_review_rss": "storyContent",
}


def _scrape_article_text(
    feed_title: str, feed_scrape_tag: str, page_title: str, page_url: str
) -> str:
    article = requests.get(page_url)
    article_content = article.content
    soup_article = BeautifulSoup(article_content, "html5lib")
    article_bodies = soup_article.find_all("div", class_=re.compile(feed_scrape_tag))
    if not article_bodies:
        print(f"Could not find article body for {page_title} at {page_url}")
        return ""

    if not os.path.exists(feed_title):
        os.makedirs(feed_title)
    page_title_filename = page_title.replace(" ", "_")
    webpage_path = f"{feed_title}/{page_title_filename}.txt"
    with open(webpage_path, "w+") as webpage_file:
        for article_body in article_bodies:
            body_text = article_body.find_all("p")
            for paragraph in body_text:
                paragraph_text = paragraph.get_text()
                webpage_file.write(f"{paragraph_text}\n")

    return ""


def parse_and_upload_rss_feed_data(feed_data_filename: str):
    """
    TODO: Filter and parse RSS feed data for coronavirus related articles
    """
    with open(feed_data_filename, "w+") as feed_data_file:
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
                    feed_title, feed_scrape_tag, article_title, article_url
                )


def main():
    parse_and_upload_rss_feed_data("feed_data.txt")


if __name__ == "__main__":
    main()
