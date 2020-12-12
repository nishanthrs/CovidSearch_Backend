#!/usr/bin/python3

# import aiohttp
from aiohttp import ClientSession
import asyncio
from bs4 import BeautifulSoup
import dask
from datetime import date
import feedparser
import html5lib
import json
from multiprocessing import Pool, Process
import os
import re
import time
from transformers import AutoModelWithLMHead, AutoTokenizer

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


async def _scrape_article_text(
    feed_session: ClientSession,
    feed_dir: str,
    feed_title: str,
    feed_scrape_tag: str,
    page_title: str,
    page_url: str,
) -> None:
    # Get text of article
    async with feed_session.get(page_url) as article_html:
        # article_content = article_html.content
        article_content = await article_html.text()
        soup_article = BeautifulSoup(article_content, "html5lib")
        article_bodies = soup_article.find_all(
            "div", class_=re.compile(feed_scrape_tag)
        )
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


async def _write_feed_metadata_and_article_text(
    feed_dir: str, feed_data_file: str, feed: str, rss_url: str
) -> None:
    rss_parsed = feedparser.parse(rss_url)
    feed_data_file.write(f"Feed: {feed}\n")
    feed_data_file.write(json.dumps(rss_parsed, indent=4))

    feed_scrape_tag = FEEDS_SCRAPE_TAG[feed]
    feed_metadata = rss_parsed["feed"]
    feed_title = feed_metadata["title"]
    feed_entries = rss_parsed["entries"]
    if not feed_entries:
        return

    for feed_entry in feed_entries:
        article_title = feed_entry["title"]
        article_url = feed_entry["link"]
        async with ClientSession() as feed_session:
            await _scrape_article_text(
                feed_session,
                feed_dir,
                feed_title,
                feed_scrape_tag,
                article_title,
                article_url,
            )


async def parse_and_upload_rss_feed_data(feed_data_filename: str) -> None:
    """
    TODO: Filter and parse RSS feed data for coronavirus related articles
    """
    curr_date_str = date.today().strftime("%m-%d-%Y")
    feed_dir = f"rss_feeds_{curr_date_str}"
    if not os.path.exists(feed_dir):
        os.makedirs(feed_dir)
    feed_data_path = os.path.join(feed_dir, feed_data_filename)

    with open(feed_data_path, "w+") as feed_data_file:
        await asyncio.gather(
            *[
                _write_feed_metadata_and_article_text(
                    feed_dir, feed_data_file, feed, rss_url
                )
                for feed, rss_url in FEEDS.items()
            ]
        )


def _summarize_news_article(article_filename: str) -> str:
    # print(f"Process: {os.getpid()}")
    model = AutoModelWithLMHead.from_pretrained("t5-base", return_dict=True)
    tokenizer = AutoTokenizer.from_pretrained("t5-base")

    with open(article_filename, "r") as article_file:
        article = article_file.read()

    # print(f"Read the file: {article_filename}")

    inputs = tokenizer.encode(
        "summarize: " + article, return_tensors="pt", max_length=512
    )
    # print(f"Encoding the file: {article_filename}")
    outputs = model.generate(
        inputs,
        max_length=150,
        min_length=100,
        length_penalty=1.2,
        num_beams=4,
        early_stopping=True,
    )
    # print(f"Summarization task output: {outputs}")
    article_summary = tokenizer.decode(outputs[0])

    return article_summary


async def main():
    """
    start_time = time.time()
    await parse_and_upload_rss_feed_data(f"feed_data.txt")
    end_time = time.time()
    # Takes around 20-30 seconds (more precisely, 24.56 seconds for 5 RSS feeds or ~170 articles)
    print(f"Execution time (async): {end_time - start_time}")
    """
    article_files = [
        # "rss_feeds_11-24-2020/Wired/Google_Is_Testing_End-to-End_Encryption_in_Android_Messages.txt",
        # "rss_feeds_11-24-2020/Wired/A_Solar-Powered_Rocket_Might_Be_Our_Interstellar_Ticket.txt",
        # "rss_feeds_11-24-2020/Wired/This_Pandemic_Must_Be_Seen.txt",
        # "rss_feeds_12-02-2020/Wired/The_Race_To_Crack_Battery_Recycling—Before_It’s_Too_Late.txt",
        "rss_feeds_12-12-2020/Wired/The_Smoking_Gun_in_the_Facebook_Antitrust_Case.txt",
        "rss_feeds_12-12-2020/Wired/Hackers_Accessed_Covid_Vaccine_Data_Through_the_EU_Regulator.txt",
        "rss_feeds_12-12-2020/Wired/The_Dark_Side_of_Big_Tech’s_Funding_for_AI_Research.txt",
        "rss_feeds_12-12-2020/New on MIT Technology Review/WhatsApp_is_limiting_message_forwarding_to_combat_coronavirus_misinformation.txt",
        "rss_feeds_12-12-2020/New on MIT Technology Review/Here_are_the_states_that_will_suffer_the_worst_hospital_bed_shortages.txt",
        "rss_feeds_12-12-2020/New on MIT Technology Review/The_coronavirus_test_that_might_exempt_you_from_social_distancing—if_you_pass.txt",
        "rss_feeds_12-12-2020/NYT > Science/F.D.A._Clears_Pfizer_Vaccine,_and_Millions_of_Doses_Will_Be_Shipped_Right_Away.txt",
        "rss_feeds_12-12-2020/NYT > Science/Earth_Is_Still_Sailing_Into_Climate_Chaos,_Report_Says,_but_Its_Course_Could_Shift.txt",
        "rss_feeds_12-12-2020/NYT > Health/Covid_Testing:_What_You_Need_to_Know.txt",
        "rss_feeds_12-12-2020/Wired/Severe_Wildfires_Are_Devastating_the_California_Condor.txt",
    ]
    start_time = time.time()

    """
    # Dask multiprocessing (runs on a single process though?!)
    # 50-55 seconds
    article_summaries = []
    for article_file in article_files:
        article_summary = dask.delayed(_summarize_news_article)(article_file)
        # article_summary.visualize()
        article_summaries.append(article_summary)
    article_summaries_futures = dask.persist(*article_summaries)
    article_summaries_res = dask.compute(*article_summaries_futures)
    # print(f"Article summaries: {article_summaries_res}")
    """

    # Multiprocessing #1
    # 1083 seconds??? wtf (on Ubuntu machine)
    # For some reason, worked fine in 40-45 seconds on FB Mac
    with Pool(4) as p:
        article_summaries = p.map(_summarize_news_article, article_files)
        print(f"Article summaries: {article_summaries}")
    p.close()
    p.join()

    """
    # Multiprocesing #2
    # Issue #1: https://stackoverflow.com/questions/50168647/multiprocessing-causes-python-to-crash-and-gives-an-error-may-have-been-in-progr
    # 40-45 seconds
    procs = []
    for article_file in article_files:
        proc = Process(target=_summarize_news_article, args=(article_file,))
        procs.append(proc)
        proc.start()

    for proc in procs:
        proc.join()
    """

    """
    List Comprehension
    # 90-95 seconds
    article_summaries = [
        _summarize_news_article(article_file) for article_file in article_files
    ]
    """

    summarization_exec_time = time.time() - start_time
    print(f"Summarization execution time: {summarization_exec_time}")


if __name__ == "__main__":
    asyncio.run(main())
