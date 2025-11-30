python manage.py runserver

celery -A automation beat --loglevel=info --scheduler django_celery_beat.schedulers:DatabaseScheduler

celery -A automation worker -l info -Q fast_queue -P threads --concurrency=10

celery -A automation worker -l info -Q heavy_queue -P threads --concurrency=5




Fields and examples 
===================

1) CRON_EXPRESSION 
================
CRON_EXPRESSION= 0,30 * * * *

SCHEDULE_TYPE   	Description	                                    CRON_EXPRESSION	        Notes
EVERY_30_MIN	    Run every 30 minutes	                        0,30 * * * *	        At minute 0 and 30 of every hour
DAILY_ONCE	        Run once daily at 7 AM              	        0 7 * * *	            Minute 0, hour 7, every day
DAILY_TWICE	        Run daily at 7 AM & 7 PM                        0 7,19 * * *	        Minute 0, hours 7 and 19
HOURLY	            Run every hour at minute 0      	            0 * * * *           	Minute 0 of every hour
WEEKLY_ONCE     	Run weekly on Monday at 9 AM	                0 9 * * 1           	Day of week = 1 (Monday)
WEEKLY_TWICE    	Run twice weekly on Mon & Thu at 9 AM	        0 9 * * 1,4         	Days 1 (Mon) & 4 (Thu)
MONTHLY_ONCE    	Run monthly on the 1st day at 10 AM	            0 10 1 * *	            Day_of_month = 1
MONTHLY_TWICE	    Run monthly on 1st & 15th day at 10 AM	        0 10 1,15 * *       	Day_of_month = 1 & 15
CUSTOM_CRON	        Any custom schedule	                            30 14 * * 1-5	        2:30 PM Monday–Fridaye



2) FILENAME 
=========
FILENAME=salers_report_:today

| Template Filename                       | Resolved Filename                     | Description                                |
| --------------------------------------- | ------------------------------------- | ------------------------------------------ |
| `salers_report_:today`                  | `salers_report_2025-11-26`            | Daily sales report for today               |
| `salers_report_:yesterday`              | `salers_report_2025-11-25`            | Daily sales report for yesterday           |
| `monthly_report_:this_month`            | `monthly_report_2025-11`              | Monthly report for current month           |
| `any_report_:month_start_to_:today`     | `any_report_2025-11-01_to_2025-11-26` | Report from start of month to today        |
| `any_report_:month_start_to_:yesterday` | `any_report_2025-11-01_to_2025-11-25` | Report from start of month to yesterday    |
| `sales_summary_:this_month`             | `sales_summary_2025-11`               | Summary for the current month              |
| `inventory_:month_start_to_:today`      | `inventory_2025-11-01_to_2025-11-26`  | Inventory report from month start to today |




3) ACTIVE 
=======
active = 1 

active 0 => task desabled 
active 1 => task enabled



4) QUEUE_NAME
==========
QUEUE_NAME = fast_queue

queue_name = "fast_queue"  => executes fast 
queue_name = "heavy_queue  => only for if heavy task    



4) RECIPIENTS , CC_EMAILS 
======================
CC_EMAILS = "example1@gmail.com , example2@gmail.com"
RECIPIENTS = "example2@gmail.com , example3@gmail.com"


6) SUBJECT 
===========
SUBJECT = "Test Email"


7) BODY
========
BODY="This is a test email body"


8) SCHEDULE_TYPE
================
SCHEDULE_TYPE= "any name to be identify"




9) CODE_TYPE
=============
CODE_TYPE = "python"
CODE_TYPE = "sql"


10) CODE 
=========
CODE = "SELECT * FROM DUAL "
CODE = "for i in range(3):
              print(i)" 





=> This can be use for sync_export_jobs tast manually 
python manage.py dbshell
from apps.dashboard.tasks import sync_export_jobs
sync_export_jobs.delay()


