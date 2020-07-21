import logging

from covidsearch_backend.covidsearch_backend.celery import app
from covidsearch_backend.covidsearch_backend.scripts.covid19_rss_feeds import (
    parse_and_upload_rss_feed_data,
)
from datetime import datetime


@app.task(name="retrieve_rss_feeds")
def retrieve_rss_feeds(rss_feeds_filename):
    parse_and_upload_rss_feed_data(rss_feeds_filename)


"""
def setup_periodic_tasks(sender, **kwargs):
    # Calls RSS feed task every 15 mins
    curr_time = datetime.now()
    num_minutes = 1
    num_seconds = num_minutes * 60
    rss_feeds_filename = f"rss_feed_{curr_time}"
    sender.add_periodic_task(
        num_seconds,
        retrieve_rss_feeds.s(rss_feeds_filename),
        name="Retrieve RSS feeds every 15 minutes.",
    )
"""
