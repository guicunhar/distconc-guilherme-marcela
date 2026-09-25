import json
import os
import signal
import socket

from confluent_kafka import Consumer


def carregar_configuracoes():
    """
    Pega as configurações definidas no docker-compose.
    """
    return {
        "bootstrap_servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "topic": os.environ["KAFKA_TOPIC"],
        "group_id": os.environ["KAFKA_CONSUMER_GROUP"],
        "consumer_id": gerar_consumer_id(),
        "temperature_limit": float(os.environ["TEMPERATURE_LIMIT"])
    }


def gerar_consumer_id():
    """
    Define o identificador do consumidor usado nos logs.

    Usa CONSUMER_ID se estiver definido. Caso contrário, usa o
    hostname do container, que é diferente para cada réplica
    criada com `docker compose up --scale`.
    """
    if "CONSUMER_ID" in os.environ:
        return os.environ["CONSUMER_ID"]
    return f"consumer-{socket.gethostname()}"


def encerrar(sinal, frame):
    """
    Trata o SIGTERM enviado pelo `docker stop`.

    Sem isso o Python é encerrado direto e o consumer.close()
    não é chamado, fazendo o Kafka demorar o session.timeout.ms
    para perceber que o consumidor saiu do grupo.
    """
    raise KeyboardInterrupt


def particoes_recebidas(consumer_id, particoes):
    """
    Chamada pelo Kafka quando o rebalanço dá partições
    para este consumidor.
    """
    numeros = [p.partition for p in particoes]
    print(f"[REBALANÇO] {consumer_id} recebeu as partições {numeros}")


def particoes_revogadas(consumer_id, particoes):
    """
    Chamada pelo Kafka quando o rebalanço tira partições
    deste consumidor (por exemplo, quando outro consumidor
    entra no grupo).
    """
    numeros = [p.partition for p in particoes]
    print(f"[REBALANÇO] {consumer_id} perdeu as partições {numeros}")


def processar_dados(dados, limite, consumer_id):
    """
    Processa os dados recebidos e verifica a temperatura.
    """
    temperatura = dados["temperatura"]
    sensor = dados["sensor"]

    print(
        f"{consumer_id} processou dados do {sensor}: "
        f"temperatura = {temperatura}"
    )

    if temperatura > limite:
        print(
            f"ALERTA: temperatura do {sensor} acima do limite "
            f"({temperatura} > {limite})"
        )


def main():
    """
    Inicia o consumidor e fica esperando novas mensagens.
    """
    config = carregar_configuracoes()

    signal.signal(signal.SIGTERM, encerrar)

    consumer = Consumer({
        "bootstrap.servers": config["bootstrap_servers"],
        "group.id": config["group_id"],
        "auto.offset.reset": "earliest"
    })

    consumer.subscribe(
        [config["topic"]],
        on_assign=lambda c, particoes: particoes_recebidas(
            config["consumer_id"], particoes
        ),
        on_revoke=lambda c, particoes: particoes_revogadas(
            config["consumer_id"], particoes
        ),
    )

    print(f"{config['consumer_id']} iniciado")

    try:
        while True:
            mensagem = consumer.poll(1.0)

            if mensagem is None:
                continue

            if mensagem.error():
                print(f"Erro: {mensagem.error()}")
                continue

            try:
                dados = json.loads(mensagem.value().decode("utf-8"))
            except json.JSONDecodeError:
                print(f"Mensagem inválida ignorada: {mensagem.value()}")
                continue

            print(
                f"{config['consumer_id']} recebeu uma mensagem "
                f"da partição {mensagem.partition()} "
                f"no offset {mensagem.offset()}"
            )

            processar_dados(
                dados,
                config["temperature_limit"],
                config["consumer_id"]
            )

    except KeyboardInterrupt:
        print(f"{config['consumer_id']} encerrando...")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()