"""Спільні налаштування Kafka для всіх скриптів.

За замовчуванням — локальний брокер з docker-compose.yml (localhost:9092, без автентифікації).
Для віддаленого кластера задайте змінні оточення:
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_USERNAME, KAFKA_PASSWORD
(тоді використовується SASL_PLAINTEXT / PLAIN).
"""
import os

MY_NAME = os.getenv("KAFKA_TOPIC_PREFIX", "polina_dyka")

kafka_config = {
    "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(","),
}
if os.getenv("KAFKA_USERNAME"):
    kafka_config.update(
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_plain_username=os.environ["KAFKA_USERNAME"],
        sasl_plain_password=os.environ["KAFKA_PASSWORD"],
    )

SENSORS_TOPIC = f"{MY_NAME}_building_sensors"
TEMPERATURE_ALERTS_TOPIC = f"{MY_NAME}_temperature_alerts"
HUMIDITY_ALERTS_TOPIC = f"{MY_NAME}_humidity_alerts"

# Порогові значення
TEMPERATURE_MAX = 40
HUMIDITY_MIN = 20
HUMIDITY_MAX = 80
