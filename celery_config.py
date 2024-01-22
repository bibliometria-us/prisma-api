# celery_config.py
from kombu import Exchange, Queue

CELERY_BROKER_URL = 'redis://localhost:6379/0'
CELERY_RESULT_BACKEND = 'redis://localhost:6379/0'
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_ACCEPT_CONTENT = ['json']
CELERY_ENABLE_UTC = True
CELERY_TIMEZONE = 'UTC'

CELERY_DEFAULT_QUEUE = 'default'
CELERY_QUEUES = (
    Queue('cargas', Exchange('cargas'), routing_key='cargas'),
)