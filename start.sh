#!/bin/bash
pip freeze > requirements.txt
if [ $DEBUG = 'true' ]
then
    sleep infinity
else
    celery -A app.celery purge
    celery multi start 4 -A app.celery --loglevel=error --concurrency=1 -Q:1 cargas -Q:2 wosjournals -Q:3 informes -Q:4 openalex && \
    gunicorn app:app -b 0.0.0.0:8001 --access-logfile logs/access.log --error-logfile logs/errors.log --timeout 600
fi

