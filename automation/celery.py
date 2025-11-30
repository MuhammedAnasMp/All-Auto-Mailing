# automation/celery.py
from __future__ import absolute_import, unicode_literals
import os
from celery import Celery
from celery.schedules import crontab

# Set the default Django settings module for the 'celery' program.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'automation.settings')

app = Celery('automation')

# Load task modules from all registered Django app configs.
app.config_from_object('django.conf:settings', namespace='CELERY')

app.autodiscover_tasks() 






app.conf.beat_schedule = {
    'run-my-task-at-7am': {
        'task': 'apps.dashboard.tasks.sync_export_jobs',
        'schedule': crontab(minute=0, hour=7),  # runs daily at 07:00 AM
        'options': {'queue': 'fast_queue'},      # optional queue
    },
}


