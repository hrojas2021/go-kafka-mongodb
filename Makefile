start-zookeeper:
	zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties

start-kafka:
	kafka-server-start /usr/local/etc/kafka/server.properties

run-confluentic-producer:
	go run confluentic/producer/main.go

run-confluentic-consumer:
	go run confluentic/consumer/main.go