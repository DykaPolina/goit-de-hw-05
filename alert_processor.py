"""Кроки 3–4. Читає building_sensors, фільтрує показники за порогами
і надсилає сповіщення в temperature_alerts / humidity_alerts.
"""
import json
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer

from configs import (
    HUMIDITY_ALERTS_TOPIC,
    HUMIDITY_MAX,
    HUMIDITY_MIN,
    SENSORS_TOPIC,
    TEMPERATURE_ALERTS_TOPIC,
    TEMPERATURE_MAX,
    kafka_config,
)

consumer = KafkaConsumer(
    SENSORS_TOPIC,
    **kafka_config,
    group_id="polina_dyka_alert_processor",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

producer = KafkaProducer(
    **kafka_config,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: str(k).encode("utf-8"),
)


def make_alert(data: dict, message: str) -> dict:
    return {
        "sensor_id": data["sensor_id"],
        "timestamp": data["timestamp"],
        "temperature": data["temperature"],
        "humidity": data["humidity"],
        "message": message,
        "alert_created_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


def send_alert(topic: str, alert: dict) -> None:
    producer.send(topic, key=alert["sensor_id"], value=alert)
    producer.flush()
    print(f"  -> sent to '{topic}': {alert['message']}")


print(f"Processing '{SENSORS_TOPIC}'. Ctrl+C to stop.")
try:
    for record in consumer:
        data = record.value
        t, h = data["temperature"], data["humidity"]
        print(
            f"Received [p{record.partition} o{record.offset}] "
            f"sensor={data['sensor_id']} t={t} h={h} ts={data['timestamp']}"
        )

        if t > TEMPERATURE_MAX:
            send_alert(
                TEMPERATURE_ALERTS_TOPIC,
                make_alert(data, f"Temperature {t}°C exceeds threshold {TEMPERATURE_MAX}°C"),
            )

        if h > HUMIDITY_MAX:
            send_alert(
                HUMIDITY_ALERTS_TOPIC,
                make_alert(data, f"Humidity {h}% exceeds threshold {HUMIDITY_MAX}%"),
            )
        elif h < HUMIDITY_MIN:
            send_alert(
                HUMIDITY_ALERTS_TOPIC,
                make_alert(data, f"Humidity {h}% is below threshold {HUMIDITY_MIN}%"),
            )
except KeyboardInterrupt:
    print("\nProcessor stopped.")
finally:
    consumer.close()
    producer.close()
