import redis
from datetime import datetime, timedelta

def populate_queue():
    redis_host = "localhost"  # Redis host
    redis_port = 6379         # Redis port
    queue_name = "task_queue" # Redis queue name

    redis_client = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=True)

    # Clear the queue if it already exists
    redis_client.delete(queue_name)

    # Generate dates for the last 30 days
    today = datetime.now() - timedelta(days=1)
    dates = [(today - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(30)]

    # Push dates into the Redis queue
    for date in dates:
        redis_client.rpush(queue_name, date)

    print(f"Added {len(dates)} tasks to the queue.")

if __name__ == "__main__":
    populate_queue()
