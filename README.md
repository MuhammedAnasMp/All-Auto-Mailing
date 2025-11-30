# Automation and Scheduling Guide

This document provides essential commands, configurations, and examples for managing automation tasks using Python, Celery, and cron expressions. It also includes templates for filenames, task activation status, queue management, email notifications, scheduling types, code snippets, and manual task execution.

---

## Basic Commands to Run Server and Workers

```bash
python manage.py runserver
```

Start the Django development server.

```bash
celery -A automation beat --loglevel=info --scheduler django_celery_beat.schedulers:DatabaseScheduler
```

Run Celery Beat with Django scheduler.

```bash
celery -A automation worker -l info -Q fast_queue -P threads --concurrency=10
```

Start a Celery worker for the `fast_queue` with 10 threads.

```bash
celery -A automation worker -l info -Q heavy_queue -P threads --concurrency=5
```

Start a Celery worker for the `heavy_queue` with 5 threads.

---

# Fields and Examples In Main Table
## 1. CRON_EXPRESSION
- Example: `0,30 * * * *`
- Description: Runs every 30 minutes at minute 0 and 30 of every hour.


| SCHEDULE_TYPE | Description                      | CRON_EXPRESSION | Notes                    |
| ------------- | -------------------------------- | --------------- | ------------------------ |
| EVERY_30_MIN  | Run every 30 minutes             | `0,30 * * * *`  | Runs at minute 0 and 30  |
| DAILY_ONCE    | Run daily at 7 AM                | `0 7 * * *`     | Minute 0, hour 7         |
| DAILY_TWICE   | Run daily at 7 AM & 7 PM         | `0 7,19 * * *`  | Hours 7 and 19           |
| HOURLY        | Run every hour                   | `0 * * * *`     | Minute 0 of every hour   |
| WEEKLY_ONCE   | Run weekly on Monday at 9 AM     | `0 9 * * 1`     | 1 = Monday               |
| WEEKLY_TWICE  | Run on Monday & Thursday at 9 AM | `0 9 * * 1,4`   | Days 1 (Mon), 4 (Thu)    |
| MONTHLY_ONCE  | Run monthly on 1st at 10 AM      | `0 10 1 * *`    | Day 1 of each month      |
| MONTHLY_TWICE | Run on 1st & 15th at 10 AM       | `0 10 1,15 * *` | Days 1 and 15            |
| CUSTOM_CRON   | Custom rule                      | `30 14 * * 1-5` | Example: 2:30 PM Mon–Fri |


## 2. FILENAME
### Dynamic filename templates automatically resolve dates.


| Template Filename                       | Resolved Filename                     | Description             |
| --------------------------------------- | ------------------------------------- | ----------------------- |
| `salers_report_:today`                  | `salers_report_2025-11-26`            | Today’s report          |
| `salers_report_:yesterday`              | `salers_report_2025-11-25`            | Yesterday’s report      |
| `monthly_report_:this_month`            | `monthly_report_2025-11`              | Current month report    |
| `any_report_:month_start_to_:today`     | `any_report_2025-11-01_to_2025-11-26` | Month start → today     |
| `any_report_:month_start_to_:yesterday` | `any_report_2025-11-01_to_2025-11-25` | Month start → yesterday |
| `sales_summary_:this_month`             | `sales_summary_2025-11`               | Current month summary   |
| `inventory_:month_start_to_:today`      | `inventory_2025-11-01_to_2025-11-26`  | Inventory report        |


## 3. ACTIVE
### Enable or disable a job.

| Value | Meaning       |
| ----- | ------------- |
| `1`   | Task enabled  |
| `0`   | Task disabled |


## 4. QUEUE_NAME
### Select appropriate execution speed.

| Queue         | Description                    |
| ------------- | ------------------------------ |
| `fast_queue`  | For fast, lightweight tasks    |
| `heavy_queue` | For heavy or long-running jobs |



## 5. RECIPIENTS , CC_EMAILS
### Comma-separated email lists:

```text 
CC_EMAILS = "example1@gmail.com, example2@gmail.com"
 RECIPIENTS = "example2@gmail.com, example3@gmail.com"
```

## 6. SUBJECT
```text
SUBJECT = "Daily Sales Report - 2025-11-26"
```

## 7. BODY

**Plain text or HTML email body**

```text
BODY = "Please find attached the latest report.\n\nBest regards,\nAutomation Team"
```


## 8. SCHEDULE_TYPE

**Free-text identifier (for your own reference)**

```text
textSCHEDULE_TYPE = "Daily Sales Export"
```

## 9. CODE_TYPE
```text
CODE_TYPE = "python"  # Execute Python code
 CODE_TYPE = "sql"     # Execute SQL query (result will be exported)
```

## 10. CODE
### if CODE_TYPE ="sql" then code must be sql
```sql
CODE = "SELECT * FROM DUAL"
```
### if CODE_TYPE ="python" then code must be python
```sql
CODE = """for i in range(3):
              print(i)"""
```

## Main Table example

| TASK_NAME          | ACTIVE | CRON_EXPRESSION | QUEUE_NAME  | CODE_TYPE | CODE                    | FILENAME_TEMPLATE                | RECIPIENTS                                                      | SUBJECT                     |
| ------------------ | ------ | --------------- | ----------- | --------- | ----------------------- | -------------------------------- | --------------------------------------------------------------- | --------------------------- |
| Daily Sales Export | 1      | 0 7 * * *       | fast_queue  | python    | print("Export Sales")   | sales_report_:today              | [sales_team@example.com](mailto:sales_team@example.com)         | Daily Sales Report - :today |
| Monthly Inventory  | 1      | 0 10 1 * *      | heavy_queue | sql       | SELECT * FROM inventory | inventory_:month_start_to_:today | [inventory_team@example.com](mailto:inventory_team@example.com) | Monthly Inventory Report    |


The Master Table defines all scheduled automation tasks. The **Daily Sales Export** task is enabled and runs daily at 7 AM via `fast_queue`, executing Python code to export sales data, generating a report named `sales_report_:today`, and emailing it to `sales_team@example.com`. The **Monthly Inventory** task is enabled and runs monthly on the 1st at 10 AM via `heavy_queue`, executing an SQL query on the inventory table, generating a report named `inventory_:month_start_to_:today`, and emailing it to `inventory_team@example.com`.  

Every day at 7 AM, the system reads the Master Table to create, update, or delete cron jobs as needed. Tasks are executed at their scheduled times, reports with dynamic filenames are generated and emailed to recipients, and execution logs are stored for auditing.


---
## Reusable Functions For CODE_TYPE Python

### 1. `fetch_data(sql_query)`

Fetches data from the database based on the provided SQL query then return df.

**Parameters:**
- `sql_query` (str): SQL query to execute.

**Returns:**
- `DataFrame`: Pandas DataFrame containing the query results.

**Example:**
```python
df = fetch_data("SELECT * FROM sales_order WHERE order_date >= '2025-11-01'")
```

### 2. `resolve_filename(file_name)`

Generates a resolved or unique filename to avoid overwriting files.

## Parameters

- `file_name` (`str`): Base file name.

## Returns

- `str`: Resolved filename string (e.g., with timestamp).

## Example

```python
resolved_filename_str = resolve_filename("Daily_Sales_Report")
 print(resolved_filename_str)
 # Example output: "Daily_Sales_Report_20251130_153045.txt"
```



### 3. `write_to_excel(df, output_path)`

Writes a DataFrame to an Excel file.

## Parameters

- `df` (`DataFrame`): Data to write.
- `output_path` (`str`): Path to save the Excel file.

## Returns

- `str`: Path to the generated Excel file.

## Example

```python
output_file = write_to_excel(df, f"{resolved_filename_str}.xlsx")
 print(output_file)
 # Example output: "Daily_Sales_Report_20251130_153045.xlsx"
```

### 4. `send_email_with_attachments(subject, body, recipient, cc, attachments)`

Sends an email with optional attachments.

## Parameters

- `subject` (`str`): Email subject line.  
- `body` (`str`): Email body (plain text or HTML).  
- `recipient` (`list` of `str`): List of recipient email addresses.  
- `cc` (`list` of `str`): List of CC email addresses.  
- `attachments` (`list` of `str`): List of file paths to attach.  

## Example

```python
send_email_with_attachments(
    subject="Daily Sales Report",
    body="Please find attached the latest report.",
    recipient=["sales@company.com"],
    cc=["manager@company.com"],
    attachments=[output_file]
)
```
## Workflow Example

This example demonstrates a complete workflow: fetching data, generating a unique filename, writing to Excel, and sending it via email with attachments.

```python
# Fetch data
df = fetch_data(sql_query)

# Resolve filename
resolved_filename_str = resolve_filename(file_name)

# Write to Excel
output_path = f"{resolved_filename_str}.xlsx"
output_file = write_to_excel(df, output_path)

# Send email with attachment
send_email_with_attachments(
    subject,
    body,
    recipient,
    cc,
    [output_file]
)
```

## Manual Trigger of `sync_export_jobs` Task

You can manually run the export job instead of waiting for 7 AM. From the main table to Django's server, run the following shell command:

```bash
python manage.py dbshell
 from apps.dashboard.tasks import sync_export_jobs
 sync_export_jobs.delay()

```



Enjoy automation!