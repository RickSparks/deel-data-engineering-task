#################################################
#
# 1. Import Libraries
#
import json
import os
import subprocess
import time
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaError



#################################################
#
# 2. Import Created Utils for DB Interaction
# 
from db_utils import (
    create_raw_schema,
    get_connection,
    upsert_customer,
    upsert_order,
    upsert_order_item,
    upsert_product,
)



#################################################
#
# 3. Config Variables
#
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

GROUP_ID = "analytics-consumer"

DBT_PROJECT_DIR = os.getenv(
    "DBT_PROJECT_DIR",
    os.path.join(os.path.dirname(__file__), "../dbt_project")
)

DBT_TRIGGER_BATCH_SIZE = int(os.getenv("DBT_TRIGGER_BATCH_SIZE", "50"))


#################################################
#
# 4. Kafka Topics to Listen to
#
TOPICS = [
    "finance_db.operations.customers",
    "finance_db.operations.products",
    "finance_db.operations.orders",
    "finance_db.operations.order_items",
]




# ----------------------------------------------------------------
# DEBEZIUM MESSAGE PARSER
# Debezium wraps every change in a payload with:
#   - op:     'r' (read/snapshot), 'c' (create), 'u' (update), 'd' (delete)
#   - before: row state before the change (null for inserts)
#   - after:  row state after the change (null for deletes)
# We always use 'after', except for deletes where we use 'before'.
# ----------------------------------------------------------------



#################################################
#
# 5. Defining Methods
#

# 5.1 Parse Debezium Messages
def parse_debezium_message(raw_value: bytes):
    """
    Returns (data_dict, op_char) or (None, None) if message should be skipped.
    """

    # Returns none if raw is none
    if raw_value is None:
        return None, None

    # Else, loads the json
    msg = json.loads(raw_value.decode("utf-8"))
    payload = msg.get("payload", msg)

    # Gets Debezium operation:
    # - r -> read
    # - c -> create
    # - u -> update
    # - d -> delete (I still keep these because it is required )
    op = payload.get("op")
    if op not in ("r", "c", "u", "d"):
        return None, None

    # If operation is a deletion, we get the row state before the change
    if op == "d":
        data = payload.get("before")
    # If not, the following row state
    else:
        data = payload.get("after")

    # If there's no data, return none
    if data is None:
        return None, None

    # Convert timestamps epoch into datetime strings for updated_at and created_at
    for key in ("updated_at", "created_at"):
        if key in data and data[key] is not None:
            data[key] = datetime.fromtimestamp(data[key] / 1_000_000, tz=timezone.utc).replace(tzinfo=None)

    # return the data and the operation type
    return data, op


# 5.2 Map Topics to the right upsert
def route_message(topic: str, data: dict, op: str, conn):

    if "customers" in topic:
        upsert_customer(conn, data, op)

    elif "products" in topic:
        upsert_product(conn, data, op)

    elif "order_items" in topic:
        upsert_order_item(conn, data, op)

    elif "orders" in topic:
        upsert_order(conn, data, op)

    else:
        print(f"[WARN] Unknown topic: {topic}")


# ----------------------------------------------------------------
# DBT RUNNER
# Calls `dbt snapshot` then `dbt run` as a subprocess.
# snapshot: handles SCD Type 2 on dimension tables.
# run:      rebuilds staging views and fact tables.
# ----------------------------------------------------------------

# 5.3 Run DBT
def run_dbt():

    print(f"\n[{datetime.now()}] Triggering dbt...")

    for cmd in [["dbt", "snapshot"], ["dbt", "run"]]:

        result = subprocess.run(
            cmd,
            cwd=DBT_PROJECT_DIR,
            capture_output=True,
            text=True,
            env={
                **os.environ,
                "DBT_PROFILES_DIR": DBT_PROJECT_DIR,
            }
        )

        status = "OK" if result.returncode == 0 else "FAILED"

        print(f"  {' '.join(cmd)} → {status}")
            
    print(f"[{datetime.now()}] dbt pass complete.\n")



#################################################
#
# 6. MAIN LOGIC
#
def main():

    # Print details
    print(f"[{datetime.now()}] Starting Analytics Consumer")
    print(f"  Kafka: {KAFKA_BOOTSTRAP}")
    print(f"  Topics: {TOPICS}")

    # Creates raw schema and tables if they don't exist
    conn = get_connection()
    create_raw_schema(conn)

    # 2. Run an initial dbt pass to build models
    run_dbt()

    # Set up a consumer
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",   # Start from the beginning of each topic
        "enable.auto.commit": True,
    })

    # Subscribe to topics
    consumer.subscribe(TOPICS)

    # Setup message counter to zero
    messages_since_last_dbt = 0

    print(f"[{datetime.now()}] Listening for CDC events...")

    # Start processing
    try:
        while True:
            # Wait a second tops for a message
            msg = consumer.poll(timeout=1.0)

            # If there's no message after timeout
            if msg is None:
                # flush batch even if small and update counter
                if messages_since_last_dbt > 0:
                    run_dbt()
                    messages_since_last_dbt = 0
                continue

            # if there's a message error
            if msg.error():
                # Check if it reached the end of the partition, if so, it's okay:
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue  # Normal: reached end of partition
                # If not, print error and continue to the next one
                print(f"[ERROR] Kafka error: {msg.error()}")
                continue

            # If there's no error call the Debezium message  parser
            data, op = parse_debezium_message(msg.value())
            if data is None:
                continue

            # Route to correct upsert function
            try:
                # route message
                route_message(msg.topic(), data, op, conn)
                # increase counter
                messages_since_last_dbt += 1
                # communicate opertion topic and data
                print(f"  [{op}] {msg.topic().split('.')[-1]} → id={list(data.values())[0]}")
            # Handle exceptions and rollback
            except Exception as e:
                print(f"[ERROR] Failed to upsert message from {msg.topic()}: {e}")
                print(f"  Data: {data}")
                conn.rollback()

            # Trigger dbt after each batch
            if messages_since_last_dbt >= DBT_TRIGGER_BATCH_SIZE:
                run_dbt()
                messages_since_last_dbt = 0

    # Assures it doesn't run forever
    except KeyboardInterrupt:
        print("\nShutting down consumer")
    
    # Clean consumer and connections
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()