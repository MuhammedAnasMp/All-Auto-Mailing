# myapp/tasks.py
from celery import shared_task
import oracledb
from email.message import EmailMessage
import smtplib
import json
import logging
import os
import pandas as pd
from datetime import datetime
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
      
        logging.StreamHandler() 
    ]
)

logger = logging.getLogger(__name__)

from django.conf import settings

def connection():
        
        username = settings.ORACLE_USERNAME
        password = settings.ORACLE_PASSWORD
        dsn = settings.ORACLE_DSN
        client_path = settings.ORACLE_CLIENTPATH
        print(username)
        print(password)
        print(dsn)
        print(client_path)
        try:
      
            oracledb.init_oracle_client(lib_dir=client_path)
       
            conn = oracledb.connect(user=username, password=password, dsn=dsn)
       
            return conn
        except oracledb.Error as e:
           
            raise
        except Exception as e:
            raise



def fetch_export_jobs(sql_query ):
    """Fetch rows from Oracle EXPORT_EMAIL_JOB table."""
    conn = connection()  # or your oracledb.connect(...)
    cursor = conn.cursor()


    cursor.execute(sql_query)
    cols = [c[0] for c in cursor.description]
    rows = cursor.fetchall()

    # Convert LOBs to strings **before closing cursor**
    result = []
    for r in rows:
        row_dict = {}
        for i, col in enumerate(cols):
            val = r[i]
            if hasattr(val, 'read'):  # if LOB
                val = val.read()     # read the full CLOB
            row_dict[col] = val
        result.append(row_dict)

    cursor.close()
    return result





@shared_task
def sync_export_jobs():
    from django_celery_beat.models import PeriodicTask, CrontabSchedule
    import json

    sql = """
        SELECT 
            ID,
            SUBJECT,
            BODY,
            RECIPIENTS,
            CC_EMAILS,
            SCHEDULE_TYPE,
            CRON_EXPRESSION,
            QUEUE_NAME,
            ACTIVE
        FROM EXPORT_EMAIL_JOB
    """

    jobs = fetch_export_jobs(sql)
    
    for job in jobs:
        job_id = job["ID"]
        schedule_type = job["SCHEDULE_TYPE"]
        queue_name = job["QUEUE_NAME"]
        cron_expr = job.get("CRON_EXPRESSION")
        description = job.get("BODY", "")
        active = job.get("ACTIVE", 0)  # 1 -> enabled, 0 -> disabled
    

        # Default schedule is None
        schedule = None

        # Only create a schedule if not ON_DEMAND and task is active
        if schedule_type != "ON_DEMAND" and active == 1:
            if not cron_expr:
                print(f"Job {job_id} has no CRON_EXPRESSION, skipping...")
                continue

            try:
                minute, hour, dom, month, dow = cron_expr.split()
            except Exception:
                print(f"Invalid CRON for job {job_id}: {cron_expr}")
                continue

            schedule, _ = CrontabSchedule.objects.get_or_create(
                minute=minute,
                hour=hour,
                day_of_month=dom,
                month_of_year=month,
                day_of_week=dow,
            )
        else:
            from django_celery_beat.models import CrontabSchedule
            never_schedule, _ = CrontabSchedule.objects.get_or_create(
                minute='0',
                hour='0',
                day_of_month='31',
                month_of_year='2',
                day_of_week='*'
            )
            schedule = never_schedule
            enabled = False

        # Enabled only if ACTIVE is 1
        enabled = True if active == 1 else False
        print(f"Job {job_id} enabled: {enabled}")

        # Create or update the periodic task
        PeriodicTask.objects.update_or_create(
            name=f"export_email_job_{job_id}",
            defaults={
                "task": "apps.dashboard.tasks.run_scheduled_export",
                "crontab": schedule,
                "args": json.dumps([job_id, queue_name]),
                "enabled": enabled,
                "description": description
            }
        )

    return {"status": "This task is scheduled to run at 7 AM every day"}


@shared_task(queue="heavy_queue")
def heavy_task():
    print("Running heavy task...")
    # Example heavy process
    import time
    for i in range(30):
        time.sleep(1)
        print(i)
    print("------------- Running heavy task completed ")



def delete_files(output_path):
    # Delete the Excel and image files if they exist
    if os.path.exists(output_path):
        os.remove(output_path)
        logger.info(f"✅ Deleted Excel file: {output_path}")
    else:
        logger.warning(f"❌ Excel file not found: {output_path}")



smtp_server = settings.SMTP_SERVER
smtp_port = settings.SMTP_PORT
smtp_username = settings.SMTP_USERNAME
smtp_password = settings.SMTP_PASSWORD
def send_email_with_attachments(subject, body, recipient, cc ,attachments):
    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = '<kuwaitalert@grandhyper.com>'
    msg['To'] = ', '.join(recipient) if isinstance(recipient, list) else recipient

    # Add CC if provided
    if cc:
        msg['Cc'] = ', '.join(cc)

    msg.set_content(body)

    for file_path in attachments:
        try:
            with open(file_path, 'rb') as f:
                file_name = os.path.basename(file_path)
                msg.add_attachment(
                    f.read(),
                    maintype='application',
                    subtype='octet-stream',
                    filename=file_name
                )
            print(f"✅ Attached {file_name}")
        except Exception as e:
            print(f"❌ Error attaching {file_path}: {e}")

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_username, smtp_password)
            # Include CC recipients in actual sending list
            all_recipients = recipient + (cc if cc else [])
            server.send_message(msg, to_addrs=all_recipients)

        print("✅ Email sent successfully.")
    except Exception as e:
        print("❌ Error sending email:", e)

    finally:
        for output_path in attachments:
            if os.path.exists(output_path):
                os.remove(output_path)
                logger.info(f"✅ Deleted Excel file: {output_path}")
            else:
                logger.warning(f"❌ Excel file not found: {output_path}")




from datetime import date, timedelta
def resolve_filename(filename = "output_file"):
    if filename is None:
        filename = "output_file"
    today = date.today()
    yesterday = today - timedelta(days=1)
    first_day_of_month = today.replace(day=1)

    filename = filename.replace(":today", today.strftime("%Y-%m-%d"))
    filename = filename.replace(":yesterday", yesterday.strftime("%Y-%m-%d"))
    filename = filename.replace(":this_month", today.strftime("%Y-%m"))
    filename = filename.replace(":month_start", first_day_of_month.strftime("%Y-%m-%d"))
    
    return filename



def fetch_data(sql_query):
    """
    Fetch data from the database and return a DataFrame.
    """
    try:
        conn = connection()  # Replace with your DB connection function
        df = pd.read_sql(sql_query, con=conn)
        conn.close()
        logger.info("Query executed and data loaded into DataFrame.")
        return df
    except Exception as e:
        logger.error(f"Failed to execute query: {e}")
        return pd.DataFrame()


def write_to_excel(df, output_path):
    """
    Write the DataFrame to an Excel file and return the absolute path.
    """
    try:
        df.to_excel(output_path, index=False)
        output_path = os.path.abspath(output_path)
        logger.info(f"✅ Data written to {output_path}")
        return output_path
    except Exception as e:
        logger.error(f"Failed to write to Excel: {e}")
        return None

@shared_task(bind=True)
def processing_fetched_code(self, job_id):   
    job_id = int(job_id)    
    sql = f"""
        SELECT 
            ID,
            SUBJECT,
            BODY,
            CODE,
            RECIPIENTS,
            CC_EMAILS,
            SCHEDULE_TYPE,
            CRON_EXPRESSION,
            QUEUE_NAME,
            ACTIVE,
            FILENAME,
            CODE_TYPE
        FROM EXPORT_EMAIL_JOB 
        WHERE ID = {job_id}
    """

    jobs = fetch_export_jobs(sql)
    
    if not jobs:
        print(f"No job found with ID {job_id}")
        return
    
    job = jobs[0]
    job_id = job["ID"]
    body = job.get("BODY", "")
    subject = job.get("SUBJECT", "")
    file_name = job.get("FILENAME", "report")
    type = job.get("CODE_TYPE", "")
    recipient_str = job.get("RECIPIENTS", "")
    cc_str = job.get("CC_EMAILS", "")    
    recipient = [r.strip() for r in recipient_str.split(",") if r.strip()]
    cc = [c.strip() for c in cc_str.split(",") if c.strip()]

    if type and  type.lower() =='python':
        python_code = job.get("CODE", "")

        try:
            exec(python_code)
        except Exception as e:
            return {"status": f"Error executing code: {e}", "job_id": job_id}
    elif type and  type.lower() =='sql':

        sql_query= job.get("CODE", "")
        df = fetch_data(sql_query)
        if df.empty:
            logger.warning("No data to write.")
            return None
        
        resolved_filename_str = resolve_filename(file_name)

        output_path = f"{resolved_filename_str}.xlsx"

        output_file =  write_to_excel(df, output_path)
        
        if output_file is None or not os.path.exists(output_file):
            print("❌ report generation failed. Email not sent.")
            return



        send_email_with_attachments( subject, body, recipient, cc, [output_file])


    else:
        return {"status": "specify the code type", "job_id": job_id}



    return {"status": "completed this ", "job_id": job_id}



@shared_task(bind=True)
def run_scheduled_export(self, job_id, queue_name):
    result = processing_fetched_code.apply_async(args=[job_id], queue=queue_name)

    return {
        "message": "Task triggered",
        "job_id": job_id,
        "queue": queue_name,
        "process_task_id": result.id,
        "periodic_task": "export_scheduler",  # if triggered by a periodic task
        "worker": self.request.hostname,       # current worker name
    }

    