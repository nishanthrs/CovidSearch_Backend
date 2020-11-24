#!/usr/bin/python3

from bs4 import BeautifulSoup
import feedparser
import html2text
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


def _clean_webpage_html(file_dir: str, local_file: str, clean_file: str) -> None:
    html_parser = html2text.HTML2Text()
    html_parser.ignore_links = True
    with open(local_file, "r") as uncleaned_webpage:
        cleaned_webpage_text = html_parser.handle(uncleaned_webpage.read())
    os.remove(local_file)

    feed_dir = file_dir
    if not os.path.exists(feed_dir):
        os.makedirs(feed_dir)
    webpage_path = f"{feed_dir}/{clean_file}"
    with open(webpage_path, "w+") as cleaned_webpage:
        cleaned_webpage.write(cleaned_webpage_text)


def _scrape_article_text(feed_title: str, page_title: str, page_url: str) -> str:
    """
    webpage_uncleaned_html = requests.get(page_url).text
    with open(page_title, "w+") as webpage_file:
        webpage_file.write(webpage_uncleaned_html)
        _clean_webpage_html(feed_title, page_title, f"final_{page_title}.txt")
    """

    article = requests.get(page_url)
    article_content = article.content
    soup_article = BeautifulSoup(article_content, "html5lib")
    article_bodies = soup_article.find_all("div", class_=re.compile("storyContent"))
    if not article_bodies:
        return ""

    article_body = article_bodies[0]
    article_text = article_body.find_all("p")
    with open(f"{page_title}.txt", "w+") as webpage_file:
        for article_paragraph in article_text:
            article_paragraph_text = article_paragraph.get_text()
            webpage_file.write(f"{article_paragraph_text}\n")

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

            feed_metadata = rss_parsed["feed"]
            feed_title = feed_metadata["title"]
            feed_entries = rss_parsed["entries"]
            if not feed_entries:
                continue

            for feed_entry in feed_entries[0:1]:
                article_title = feed_entry["title"]
                print(f"Article title: {article_title}")
                article_url = feed_entry["link"]
                print(f"Article url: {article_url}")
                _scrape_article_text(feed_title, article_title, article_url)


def main():
    parse_and_upload_rss_feed_data("feed_data.txt")


if __name__ == "__main__":
    main()
