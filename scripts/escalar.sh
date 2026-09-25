#!/usr/bin/env bash
#
# Demonstra a elasticidade do sistema: muda o número de réplicas de um
# serviço com o sistema rodando. Para consumidores, mostra o rebalanço
# que redistribui as partições entre as réplicas.
#
# Uso: scripts/escalar.sh <quantidade> [servico]   (padrão: consumer)
#
# Exemplos:
#   scripts/escalar.sh 4             4 consumidores
#   scripts/escalar.sh 8 producer    8 sensores
#
# Variáveis de ambiente opcionais:
#   ESPERA   segundos para observar o rebalanço (padrão: 15)
#   LOG_DIR  pasta onde salvar o log            (padrão: logs)

set -e

if [ -z "$1" ]; then
    echo "Uso: $0 <quantidade> [servico]"
    exit 1
fi

QUANTIDADE=$1
SERVICO=${2:-consumer}
ESPERA=${ESPERA:-15}
LOG_DIR=${LOG_DIR:-logs}

mkdir -p "$LOG_DIR"
ARQUIVO="$LOG_DIR/escalar-$SERVICO-$QUANTIDADE-$(date +%Y%m%d-%H%M%S).log"
INICIO=$(date -u +%Y-%m-%dT%H:%M:%SZ)

{
    echo "===== $(date) - Escalando $SERVICO para $QUANTIDADE réplicas"
    # --no-recreate mantém as réplicas que já estão rodando.
    docker compose up -d --no-recreate --scale "$SERVICO=$QUANTIDADE" "$SERVICO"
    sleep "$ESPERA"

    echo
    echo "===== Réplicas de $SERVICO rodando"
    docker compose ps "$SERVICO"

    if [ "$SERVICO" = "consumer" ]; then
        echo
        echo "===== Eventos de rebalanço"
        docker compose logs -t --since "$INICIO" consumer | grep "REBALANÇO" || true
    fi
} 2>&1 | tee "$ARQUIVO"

echo
echo "Log salvo em $ARQUIVO"
