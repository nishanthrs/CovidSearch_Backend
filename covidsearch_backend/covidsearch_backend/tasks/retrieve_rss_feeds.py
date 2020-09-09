import logging
import sys

# Set to directory from where command is run
sys.path.append(".")

from datetime import datetime
from redis import Redis
from rq import Queue
from rq_scheduler import Scheduler
from covidsearch_backend.settings import REDIS_HOST, REDIS_PORT
from covidsearch_backend.tasks.rss_feed_funcs import parse_and_upload_rss_feed_data

RSS_FEEDS_FILENAME = f"rss_feeds"

rss_feeds_scheduler = Scheduler(connection=Redis(host=REDIS_HOST, port=REDIS_PORT))
rss_feeds_scheduler.schedule(
    scheduled_time=datetime.utcnow(),  # Time for first execution, in UTC timezone
    func=parse_and_upload_rss_feed_data,  # Function to be queued
    args=[RSS_FEEDS_FILENAME],  # Arguments passed into function when executed
    interval=20,  # Time before the function is called again, in seconds
    repeat=None,  # Repeat this number of times (None means repeat forever)
)

