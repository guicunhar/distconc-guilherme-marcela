#!/usr/bin/env bash
#
# Simula a falha de um consumidor e mostra o rebalanço: as partições
# dele são redistribuídas entre os consumidores que continuam no ar.
#
# Uso: scripts/falha-consumidor.sh [stop|kill]   (padrão: stop)
#
#   stop  saída limpa (SIGTERM): o consumidor avisa o grupo ao sair
#         e o rebalanço é quase imediato.
#   kill  queda abrupta (SIGKILL): o Kafka só percebe depois do
#         session.timeout.ms (45 s por padrão).
#
# Variáveis de ambiente opcionais:
#   ESPERA   segundos para observar o rebalanço (padrão: 10 no stop, 60 no kill)
#   LOG_DIR  pasta onde salvar o log            (padrão: logs)

set -e

MODO=${1:-stop}
LOG_DIR=${LOG_DIR:-logs}

case "$MODO" in
    stop) ESPERA=${ESPERA:-10} ;;
    kill) ESPERA=${ESPERA:-60} ;;
    *) echo "Modo inválido: $MODO (use stop ou kill)"; exit 1 ;;
esac

# Escolhe o primeiro container do serviço consumer que está rodando.
ALVO=$(docker ps --filter "label=com.docker.compose.service=consumer" \
    --format "{{.Names}}" | sort | head -n 1)

if [ -z "$ALVO" ]; then
    echo "Nenhum consumidor rodando. Rode 'make up' antes."
    exit 1
fi

mkdir -p "$LOG_DIR"
ARQUIVO="$LOG_DIR/falha-consumidor-$MODO-$(date +%Y%m%d-%H%M%S).log"
INICIO=$(date -u +%Y-%m-%dT%H:%M:%SZ)

{
    echo "===== $(date) - Derrubando $ALVO com docker $MODO"
    docker "$MODO" "$ALVO"

    echo "Aguardando $ESPERA s para observar o rebalanço..."
    sleep "$ESPERA"

    echo
    echo "===== Eventos de rebalanço desde a falha"
    docker compose logs -t --since "$INICIO" consumer | grep "REBALANÇO" || true

    echo
    echo "===== $(date) - Religando $ALVO"
    docker start "$ALVO"
    sleep 10

    echo
    echo "===== Eventos de rebalanço após a volta"
    docker compose logs -t --since "$INICIO" consumer | grep "REBALANÇO" || true
} 2>&1 | tee "$ARQUIVO"

echo
echo "Log salvo em $ARQUIVO"
