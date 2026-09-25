#!/usr/bin/env bash
#
# Simula a queda de um broker Kafka e mostra que o sistema continua
# funcionando: as partições que ele liderava ganham um novo líder e
# produtores e consumidores seguem trabalhando.
#
# Uso: scripts/falha-broker.sh [broker]   (padrão: kafka2)
#
# Variáveis de ambiente opcionais:
#   KAFKA_TOPIC  tópico a ser descrito        (padrão: dados-sensores)
#   ESPERA       segundos com o broker fora  (padrão: 20)
#   LOG_DIR      pasta onde salvar o log     (padrão: logs)

set -e

BROKER=${1:-kafka2}
TOPICO=${KAFKA_TOPIC:-dados-sensores}
ESPERA=${ESPERA:-20}
LOG_DIR=${LOG_DIR:-logs}

# Usa um broker que continua no ar para consultar o estado do tópico.
for b in kafka1 kafka2 kafka3; do
    if [ "$b" != "$BROKER" ]; then
        CONSULTA=$b
        break
    fi
done

descrever_topico() {
    docker exec "$CONSULTA" kafka-topics \
        --bootstrap-server "$CONSULTA:9092" \
        --describe --topic "$TOPICO"
}

mkdir -p "$LOG_DIR"
ARQUIVO="$LOG_DIR/falha-broker-$(date +%Y%m%d-%H%M%S).log"

{
    echo "===== $(date) - Estado do tópico ANTES da falha"
    descrever_topico

    echo
    echo "===== $(date) - Parando o broker $BROKER"
    docker stop "$BROKER"
    sleep "$ESPERA"

    echo
    echo "===== $(date) - Estado do tópico COM $BROKER FORA"
    descrever_topico

    echo
    echo "===== Últimas mensagens dos produtores (devem continuar enviando)"
    docker compose logs --tail 6 producer

    echo
    echo "===== Últimas mensagens dos consumidores (devem continuar processando)"
    docker compose logs --tail 6 consumer

    echo
    echo "===== $(date) - Religando o broker $BROKER"
    docker start "$BROKER"
    sleep "$ESPERA"

    echo
    echo "===== $(date) - Estado do tópico DEPOIS da volta de $BROKER"
    descrever_topico
} 2>&1 | tee "$ARQUIVO"

echo
echo "Log salvo em $ARQUIVO"
