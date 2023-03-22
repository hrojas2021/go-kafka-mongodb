run-containers:
	docker compose up --build -d

start-zookeeper:
	zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties

start-kafka:
	kafka-server-start /usr/local/etc/kafka/server.properties

run-producer:
	go run cmd/producer/main.go

run-consumer:
	go run cmd/consumer/main.go