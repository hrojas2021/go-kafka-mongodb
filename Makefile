init-containers:
	docker compose up --build -d

start-zookeeper:
	zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties

start-kafka:
	kafka-server-start /usr/local/etc/kafka/server.properties

run-producer:
	go run cmd/producer/main.go

run-consumer:
	go run cmd/consumer/main.go

stop-containers:
	docker compose down --remove-orphans

send-request:
	curl --header "Content-Type: application/json" \
  --request POST \
  --data '{"title": "Golang Boss Engineer in Docker","description": "This a Boss position for Docker","company": "Sarama Docker","salary": "70.000"}' \
  http://localhost:9600/jobs
