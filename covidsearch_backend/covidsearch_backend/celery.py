from __future__ import absolute_import, unicode_literals

import os

from celery import Celery
from celery.schedules import crontab

# from scripts.covid19_rss_feeds import parse_and_upload_rss_feed_data
from covidsearch_backend.scripts.covid19_rss_feeds import parse_and_upload_rss_feed_data
from datetime import datetime

# set the default Django settings module for the 'celery' program.
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "covidsearch_backend.settings")

app = Celery("covidsearch_backend")

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.
# app.config_from_object("django.conf:settings", namespace="CELERY")

"""https://docs.celeryproject.org/en/stable/userguide/periodic-tasks.html#crontab-schedules"""


@app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    # Calls RSS feed task every 15 mins
    curr_time = datetime.now()
    num_minutes = 15
    num_seconds = num_minutes * 60
    rss_feeds_filename = f"rss_feed_{curr_time}"
    sender.add_periodic_task(
        num_seconds,
        retrieve_rss_feeds.s(rss_feeds_filename),
        name="Retrieve RSS feeds every 15 minutes.",
    )

    # Executes every Monday morning at 8:00 a.m.
    """
    sender.add_periodic_task(
        crontab(hour=8, minute=0, day_of_week=1), retrieve_rss_feeds.s(rss_feeds_filename),
    )
    """


@app.task
def retrieve_rss_feeds(rss_feeds_filename):
    parse_and_upload_rss_feed_data(rss_feeds_filename)
