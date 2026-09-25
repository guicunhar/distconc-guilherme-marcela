import json
import os
import random
import time
from datetime import datetime

from confluent_kafka import Producer


def carregar_configuracoes():
    """
    Carrega as configurações do produtor por meio das
    variáveis de ambiente definidas no docker-compose.yml.
    """
    return {
        "bootstrap_servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "topic": os.environ["KAFKA_TOPIC"],
        "sensor_id": os.environ["SENSOR_ID"],
        "acks": os.environ.get("KAFKA_ACKS", "all"),
        "interval": float(os.environ["SENSOR_INTERVAL_SECONDS"]),
    }


def gerar_dados(sensor_id):
    """
    Gera dados simulados de um sensor industrial.
    """
    return {
        "sensor": sensor_id,
        "timestamp": datetime.now().isoformat(),
        "temperatura": round(random.uniform(20, 100), 2),
        "vibracao": round(random.uniform(0, 10), 2),
        "energia": round(random.uniform(100, 500), 2),
    }


def confirmar_entrega(erro, mensagem):
    """
    Informa se a mensagem foi entregue corretamente ao Kafka.
    """
    if erro is not None:
        print(f"Erro ao enviar mensagem: {erro}")
    else:
        print(
            f"Mensagem enviada para partição {mensagem.partition()} "
            f"no offset {mensagem.offset()}"
        )


def main():
    """
    Inicializa o produtor e envia dados simulados
    continuamente para o Kafka.
    """
    config = carregar_configuracoes()

    producer = Producer(
        {
            "bootstrap.servers": config["bootstrap_servers"],
            "acks": config["acks"],
        }
    )

    print(f"Sensor iniciado: {config['sensor_id']}")

    while True:
        dados = gerar_dados(config["sensor_id"])

        print(f"Enviando: {dados}")

        producer.produce(
            topic=config["topic"],
            key=config["sensor_id"].encode("utf-8"),
            value=json.dumps(dados).encode("utf-8"),
            callback=confirmar_entrega,
        )

        producer.poll(0)

        time.sleep(config["interval"])


if __name__ == "__main__":
    main()