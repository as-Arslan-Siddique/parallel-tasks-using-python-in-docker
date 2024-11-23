import os
import time
import uuid
import redis
import concurrent.futures
from lib.google_sheets.connector import GoogleSheetsConnector

def push_data_to_bq(date):
    """
    Push data for a specific date to BigQuery.
    Args:
        date (str): The date for which data will be pushed (YYYY-MM-DD).
    """
    try:
        spreadsheet_id = "1Hl_QrUtkSZgNSg1MIvPukX48JATHCx1KXTErDEI7osg"
        sheet_name = "Sheet1"

        gSheetsObj = GoogleSheetsConnector()

        ranger = "{}".format(sheet_name)
        _id = str(uuid.uuid4())
        final_list = [
            {
                "id": _id,
                "date": date
            }
        ]
        insert_response = gSheetsObj.insert_rows_from_dicts(spreadsheet_id, ranger, final_list)
        print(insert_response)
        # print(f"Start pushing data for {date}, and id: {_id}")
        # # Simulate time-consuming work
        # time.sleep(5)  # Replace with actual data-pushing logic
        # # insert_data_to_bq(date)  # Your function to insert data into BQ
        # print(f"Data push completed for {date}, and id: {_id}")
    except Exception as e:
        print(f"Error for {date}: {e}")

def run_worker():
    # Number of workers per container (default: 10)
    max_workers = int(os.getenv("WORKERS", 10))
    
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", 6379))
    queue_name = "task_queue"
    
    redis_client = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=True)

    def get_task():
        """Retrieve a task from Redis."""
        return redis_client.lpop(queue_name)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        while True:
            # Submit up to max_workers tasks
            while len(futures) < max_workers:
                date = get_task()
                if date is None:
                    break
                future = executor.submit(push_data_to_bq, date)
                futures.append(future)

            # Check for completed tasks
            done, futures = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
            futures = list(futures)  # Convert back to list

            # Remove completed futures
            futures = [f for f in futures if not f.done()]

            # Exit if no tasks remain and all futures are done
            if not futures and redis_client.llen(queue_name) == 0:
                break

    print("Worker has finished processing all tasks.")

if __name__ == "__main__":
    run_worker()
