"""Крок 1. Створення трьох топіків."""
import time

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from configs import (
    HUMIDITY_ALERTS_TOPIC,
    MY_NAME,
    SENSORS_TOPIC,
    TEMPERATURE_ALERTS_TOPIC,
    kafka_config,
)

admin_client = KafkaAdminClient(**kafka_config)

topics = [
    NewTopic(name=name, num_partitions=2, replication_factor=1)
    for name in (SENSORS_TOPIC, TEMPERATURE_ALERTS_TOPIC, HUMIDITY_ALERTS_TOPIC)
]

for topic in topics:
    try:
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"Topic '{topic.name}' created successfully.")
    except TopicAlreadyExistsError:
        print(f"Topic '{topic.name}' already exists.")

time.sleep(1)  # даємо брокеру оновити метадані, інакше щойно створений топік може не потрапити в список
print("\nMy topics:")
[print(topic) for topic in admin_client.list_topics() if MY_NAME in topic]

admin_client.close()
