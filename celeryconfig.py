
from celery import Celery
from dotenv import load_dotenv
import os
from celery.schedules import crontab

load_dotenv()
MQ_BROKER = os.getenv('MQ_BROKER')

TIME_SCHEDULE_HOUR = os.getenv('TIME_SCHEDULE_HOUR')
TIME_SCHEDULE_MIN = os.getenv('TIME_SCHEDULE_MIN')

if not TIME_SCHEDULE_HOUR:
    TIME_SCHEDULE_HOUR='17'

if not TIME_SCHEDULE_MIN:
    TIME_SCHEDULE_MIN='0'

# Use RabbitMQ as the broker
app = Celery('celeryconfig', broker=MQ_BROKER)

# Optional: specify timezone for periodic tasks
app.conf.timezone = 'Asia/Kolkata'
app.conf.enable_utc = False

# Celery Beat settings (scheduling the task daily)
app.conf.beat_schedule = {
    'Gomati-meters-daily-data-csv': {
        'task': 'tasks.run_main',
        'schedule': crontab(hour=int(TIME_SCHEDULE_HOUR), minute=int(TIME_SCHEDULE_MIN)),
        'args': (),
    },
}



