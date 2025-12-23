from django.contrib import admin
from django_celery_beat.models import PeriodicTask
from django_celery_beat.admin import PeriodicTaskAdmin as BeatPeriodicTaskAdmin


class CustomPeriodicTaskAdmin(BeatPeriodicTaskAdmin):
    list_display = (
        'description',
        'name',
        'enabled',
        'interval',
        'crontab',
        'last_run_at',
        'total_run_count',
    )


# Replace the existing admin
admin.site.unregister(PeriodicTask)
admin.site.register(PeriodicTask, CustomPeriodicTaskAdmin)
