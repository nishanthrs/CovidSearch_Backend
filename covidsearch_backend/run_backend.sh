#!/bin/bash

python3 manage.py makemigrations
python3 manage.py migrate
gunicorn covidsearch_backend.wsgi -b 0.0.0.0:5000 -w 4
