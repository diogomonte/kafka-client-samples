Consumer/Producer kafka samples
========

Setup environment:
```
cd env
docker-compose up
```

Create kafka topic:
```
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic first-topic
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic second-topic
```