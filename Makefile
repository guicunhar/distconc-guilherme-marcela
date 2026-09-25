# Atalhos para operar o sistema de monitoramento de sensores.
# Uso: make <alvo>   (ex.: make up, make falha-broker BROKER=kafka3)

BROKER ?= kafka2
N ?= 4
SERVICO ?= consumer
TOPICO ?= dados-sensores

.PHONY: up rodar down limpar status logs logs-produtores logs-consumidores \
        logs-rebalanco salvar-logs alertas falha-broker falha-consumidor \
        queda-consumidor escalar

## Sobe todo o sistema (brokers, sensores e consumidores)
up:
	docker compose up -d --build

## Sobe o sistema e acompanha sensores e consumidores juntos, ao vivo,
## gravando ao mesmo tempo em logs/execucao.log
rodar: up
	mkdir -p logs
	docker compose logs -f -t producer consumer | tee logs/execucao.log

## Para e remove os containers (os dados do Kafka são perdidos)
down:
	docker compose down

## Remove containers, logs gerados e alertas salvos
limpar: down
	rm -rf logs/*.log dados/

## Mostra os containers e o estado das partições do tópico
status:
	docker compose ps
	docker exec kafka1 kafka-topics --bootstrap-server kafka1:9092 \
		--describe --topic $(TOPICO)

logs:
	docker compose logs -f

logs-produtores:
	docker compose logs -f producer

logs-consumidores:
	docker compose logs -f consumer

## Mostra só os eventos de rebalanço dos consumidores
logs-rebalanco:
	docker compose logs -f -t consumer | grep --line-buffered REBALANÇO

## Salva os logs atuais em logs/ (para o relatório)
salvar-logs:
	mkdir -p logs
	docker compose logs -t consumer | grep REBALANÇO > logs/rebalanco.log
	docker compose logs -t consumer > logs/consumidores.log
	docker compose logs -t producer > logs/produtores.log
	docker compose logs -t kafka1 kafka2 kafka3 > logs/brokers.log
	@echo "Logs salvos em logs/"

## Acompanha os alertas salvos pelos consumidores
alertas:
	tail -f dados/alertas.jsonl

## Derruba um broker e mostra o failover (make falha-broker BROKER=kafka3)
falha-broker:
	bash scripts/falha-broker.sh $(BROKER)

## Para um consumidor de forma limpa e mostra o rebalanço
falha-consumidor:
	bash scripts/falha-consumidor.sh stop

## Mata um consumidor de forma abrupta (rebalanço após o timeout)
queda-consumidor:
	bash scripts/falha-consumidor.sh kill

## Muda o número de réplicas (make escalar N=5 SERVICO=producer)
escalar:
	bash scripts/escalar.sh $(N) $(SERVICO)
