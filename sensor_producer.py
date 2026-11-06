"""Крок 2. Імітація одного датчика: періодично надсилає показники в building_sensors.

Один запуск = один датчик. Щоб імітувати кілька датчиків, запустіть скрипт кілька разів.
"""
import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

from configs import SENSORS_TOPIC, kafka_config

SEND_INTERVAL_SEC = 2

sensor_id = random.randint(1000, 9999)  # сталий для одного запуску

producer = KafkaProducer(
    **kafka_config,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8"),
)

print(f"Sensor {sensor_id} started, sending to '{SENSORS_TOPIC}'. Ctrl+C to stop.")
try:
    while True:
        data = {
            "sensor_id": sensor_id,
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "temperature": round(random.uniform(25, 45), 1),
            "humidity": round(random.uniform(15, 85), 1),
        }
        # ключ = sensor_id: повідомлення одного датчика потрапляють в одну партицію (порядок зберігається)
        producer.send(SENSORS_TOPIC, key=sensor_id, value=data)
        producer.flush()
        print(f"Sent: {data}")
        time.sleep(SEND_INTERVAL_SEC)
except KeyboardInterrupt:
    print(f"\nSensor {sensor_id} stopped.")
finally:
    producer.close()
