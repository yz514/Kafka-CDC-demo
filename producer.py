import time
import json
import psycopg
from confluent_kafka import Producer

# Kafka config
KAFKA_BROKER = "localhost:39092"
TOPIC_NAME = "emp_sync"

# PostgreSQL config
PG_HOST = "localhost"
PG_PORT = 5433
PG_DB = "srcdb"
PG_USER = "dev"
PG_PASSWORD = "dev"

def get_pg_connection():
    return psycopg.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        autocommit=True
    )

# Initialize confluent_kafka Producer 
producer = Producer({'bootstrap.servers': KAFKA_BROKER})

print("Producer started, watching CDC changes...")

def delivery_report(err, msg):
    """Delivery callback"""
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Sent to Kafka [{msg.topic()} @ partition {msg.partition()}]: {msg.value().decode('utf-8')}")

def send_to_kafka(record):
    producer.produce(
        TOPIC_NAME,
        value=json.dumps(record).encode('utf-8'),
        callback=delivery_report
    )
    producer.poll(0)  

def snapshot_phase(conn):
    print("Starting snapshot sync...")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT emp_id, first_name, last_name, dob, city, salary
            FROM employees
            ORDER BY emp_id ASC;
        """)
        rows = cur.fetchall()
        for row in rows:
            record = {
                "cdc_id": 0,
                "emp_id": row[0],
                "first_name": row[1],
                "last_name": row[2],
                "dob": str(row[3]),
                "city": row[4],
                "salary": row[5],
                "action": "snapshot"
            }
            send_to_kafka(record)
        producer.flush()
    print(f"Snapshot phase completed ({len(rows)} records sent)")

def stream_phase(conn):
    print("Entering stream phase (real-time CDC)...")
    last_processed_cdc_id = 0
    while True:
        try:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT cdc_id, emp_id, first_name, last_name, dob, city, salary, action
                    FROM emp_cdc
                    WHERE cdc_id > {last_processed_cdc_id}
                    ORDER BY cdc_id ASC;
                """)
                rows = cur.fetchall()

                for row in rows:
                    record = {
                        "cdc_id": row[0],
                        "emp_id": row[1],
                        "first_name": row[2],
                        "last_name": row[3],
                        "dob": str(row[4]),
                        "city": row[5],
                        "salary": row[6],
                        "action": row[7]
                    }
                    send_to_kafka(record)
                    last_processed_cdc_id = row[0]

                if rows:
                    producer.flush()
            time.sleep(2)

        except Exception as e:
            print(f"Error in stream loop: {e}")
            time.sleep(5)
            conn = get_pg_connection()

if __name__ == "__main__":
    conn = get_pg_connection()
    try:
        snapshot_phase(conn)
        stream_phase(conn)
    except KeyboardInterrupt:
        print("\nStopped by user.")
    except Exception as e:
        print(f"Producer crashed: {e}")
    finally:
        print("Flushing pending messages & closing producer...")
        producer.flush()   # push all the records
        producer.close()   # close producer
        conn.close()       # close connection
        print("Producer exited cleanly.")
