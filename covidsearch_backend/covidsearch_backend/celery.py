from __future__ import absolute_import, unicode_literals

import os

from celery import Celery
from celery.schedules import crontab


# set the default Django settings module for the 'celery' program.
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "covidsearch_backend.settings")

app = Celery("covidsearch_backend")
app.config_from_object("django.conf:settings")

app.autodiscover_tasks()

app.conf.beat_schedule = {
    "retrieve_rss_feed_data": {
        "task": "tasks.retrieve_rss_feeds",
        "schedule": crontab(minute="*/1"),
    },
}

# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.
# app.config_from_object("django.conf:settings", namespace="CELERY")

"""https://docs.celeryproject.org/en/stable/userguide/periodic-tasks.html#crontab-schedules"""
