import json
import psycopg
from confluent_kafka import Consumer, KafkaException

# Kafka config
KAFKA_BROKER = "localhost:39092"
TOPIC_NAME = "emp_sync"
GROUP_ID = "emp_sync_group"

# PostgreSQL config
PG_HOST = "localhost"
PG_PORT = 5434
PG_DB = "destdb"
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

# Initialize Kafka Consumer
consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,  # auto offset
})


consumer.subscribe([TOPIC_NAME])
print(f"🚀 Consumer subscribed to topic '{TOPIC_NAME}' and listening for CDC events...")


def process_message(msg_value, conn):
    record = json.loads(msg_value)
    action = record.get("action")
    emp_id = record.get("emp_id")

    with conn.cursor() as cur:
        if action in ("insert", "snapshot", "update"):  #UPSERT
            cur.execute("""
                INSERT INTO employees (emp_id, first_name, last_name, dob, city, salary)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (emp_id) DO UPDATE
                SET first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    dob = EXCLUDED.dob,
                    city = EXCLUDED.city,
                    salary = EXCLUDED.salary;
            """, (emp_id, record["first_name"], record["last_name"], record["dob"], record["city"], record["salary"]))
            
            action_label = "UPSERT" if action in ("insert", "snapshot") else "UPDATE"
            print(f"{action_label}: emp_id={emp_id}")

        elif action == "delete":
            cur.execute("DELETE FROM employees WHERE emp_id = %s;", (emp_id,))
            print(f"DELETED: emp_id={emp_id}")
        
        else:
            print(f"Unknown action '{action}' for emp_id={emp_id}")


def main():
    conn = get_pg_connection()
    try:
        while True:
            msg = consumer.poll(1.0)  
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            msg_value = msg.value().decode("utf-8")
            process_message(msg_value, conn)

    except KeyboardInterrupt:
        print("\n Stopped by user.")
    finally:
        consumer.close()
        conn.close()

if __name__ == "__main__":
    main()