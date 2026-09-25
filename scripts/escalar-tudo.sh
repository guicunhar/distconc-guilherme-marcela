#!/usr/bin/env bash
#
# Muda ao mesmo tempo o número de sensores e de consumidores, com o
# sistema rodando, e mostra o rebalanço dos consumidores.
#
# Uso: scripts/escalar-tudo.sh <sensores> <consumidores>
#
# Exemplo:
#   scripts/escalar-tudo.sh 8 5    8 sensores e 5 consumidores
#
# Variáveis de ambiente opcionais:
#   ESPERA   segundos para observar o rebalanço (padrão: 15)
#   LOG_DIR  pasta onde salvar o log            (padrão: logs)

set -e

if [ -z "$1" ] || [ -z "$2" ]; then
    echo "Uso: $0 <sensores> <consumidores>"
    exit 1
fi

SENSORES=$1
CONSUMIDORES=$2
ESPERA=${ESPERA:-15}
LOG_DIR=${LOG_DIR:-logs}

mkdir -p "$LOG_DIR"
ARQUIVO="$LOG_DIR/escalar-tudo-$SENSORES-sensores-$CONSUMIDORES-consumidores-$(date +%Y%m%d-%H%M%S).log"
INICIO=$(date -u +%Y-%m-%dT%H:%M:%SZ)

{
    echo "===== $(date) - Escalando para $SENSORES sensores e $CONSUMIDORES consumidores"
    # Um único comando muda os dois serviços; --no-recreate mantém as
    # réplicas que já estão rodando.
    docker compose up -d --no-recreate \
        --scale "producer=$SENSORES" \
        --scale "consumer=$CONSUMIDORES" \
        producer consumer
    sleep "$ESPERA"

    echo
    echo "===== Réplicas rodando"
    docker compose ps producer consumer

    echo
    echo "===== Eventos de rebalanço"
    docker compose logs -t --since "$INICIO" consumer | grep "REBALANÇO" || true
} 2>&1 | tee "$ARQUIVO"

echo
echo "Log salvo em $ARQUIVO"
