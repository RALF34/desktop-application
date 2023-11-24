from celery import Celery
from celery.schedules import crontab
import crud

app = Celery()
app.conf.update(timezone="Europe/Paris",enable_utc=True)
app.conf.beat_schedule = {"updates":{"task":"crud.update_database","schedule":crontab(hour=1,minute=0)}}
