# celery_config.py
import os

# Connects locally to Memurai running default port 6379
BROKER_URL = 'redis://127.0.0.1:6379/0'
RESULT_BACKEND = 'redis://127.0.0.1:6379/0'

# Standard payload serializations
TASK_SERIALIZER = 'json'
RESULT_SERIALIZER = 'json'
ACCEPT_CONTENT = ['json']
TIMEZONE = 'UTC'
ENABLE_UTC = True