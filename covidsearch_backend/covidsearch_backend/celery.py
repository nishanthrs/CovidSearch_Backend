from __future__ import absolute_import, unicode_literals

import os

from celery import Celery

# set the default Django settings module for the 'celery' program.
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "covidsearch_backend.settings")

app = Celery("covidsearch_backend.tasks")
app.config_from_object("django.conf:settings")

app.autodiscover_tasks()


@app.task(bind=True)
def debug_task(self):
    print("CALLING REQUEST JIVE MOTHERF")


# Using a string here means the worker doesn't have to serialize
# the configuration object to child processes.
# - namespace='CELERY' means all celery-related configuration keys
#   should have a `CELERY_` prefix.
# app.config_from_object("django.conf:settings", namespace="CELERY")

"""https://docs.celeryproject.org/en/stable/userguide/periodic-tasks.html#crontab-schedules"""
