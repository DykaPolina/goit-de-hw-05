"""Крок 5. Читає temperature_alerts і humidity_alerts та виводить сповіщення на екран."""
import json

from kafka import KafkaConsumer

from configs import HUMIDITY_ALERTS_TOPIC, TEMPERATURE_ALERTS_TOPIC, kafka_config

consumer = KafkaConsumer(
    TEMPERATURE_ALERTS_TOPIC,
    HUMIDITY_ALERTS_TOPIC,
    **kafka_config,
    group_id="polina_dyka_alerts_consumer",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
)

print(f"Listening to '{TEMPERATURE_ALERTS_TOPIC}' and '{HUMIDITY_ALERTS_TOPIC}'. Ctrl+C to stop.")
try:
    for record in consumer:
        a = record.value
        kind = "TEMPERATURE" if record.topic == TEMPERATURE_ALERTS_TOPIC else "HUMIDITY"
        print(
            f"[{kind} ALERT] sensor={a['sensor_id']} time={a['timestamp']} "
            f"t={a['temperature']}°C h={a['humidity']}% | {a['message']}"
        )
except KeyboardInterrupt:
    print("\nConsumer stopped.")
finally:
    consumer.close()
