from kafka_custom import KafkaReader

reader = KafkaReader(bootstrap_servers="localhost:9092",topic='testDuong', group_id='test-group')

data = reader.poll(timeout_ms=100, max_records=5)

print(data)