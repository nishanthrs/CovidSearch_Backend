#!/usr/bin/python3

import feedparser
from pprint import pprint

FEEDS = {
    "nyt_health": "https://rss.nytimes.com/services/xml/rss/nyt/Health.xml",
    "nyt_science": "https://rss.nytimes.com/services/xml/rss/nyt/Science.xml",
    "nyt_travel": "https://rss.nytimes.com/services/xml/rss/nyt/Travel.xml",
    "wsj_lifestyle": "https://feeds.a.dj.com/rss/RSSLifestyle.xml",
    "wsj_world_news": "https://feeds.a.dj.com/rss/RSSWorldNews.xml",
    "wired_rss": "https://www.wired.com/feed/rss",
}


def parse_and_upload_rss_feed_data(feed_data_filename):
    """
    TODO: Filter and parse RSS feed data for coronavirus related articles
    """
    for feed, url in FEEDS.items():
        rss_parsed = feedparser.parse(url)
        with open(feed_data_filename, "w") as feed_data_file:
            pprint(
                f"Parsed RSS data feed of {feed}: {rss_parsed}", stream=feed_data_file
            )
            print("Keys: ", rss_parsed.keys())


def main():
    parse_and_upload_rss_feed_data("feed_data.txt")


if __name__ == "__main__":
    main()
