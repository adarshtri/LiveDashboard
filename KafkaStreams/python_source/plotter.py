from kafka import KafkaConsumer
consumer = KafkaConsumer('gorupbyStreamValue')

for msg in consumer:
	print(msg.value.decode('utf-8'))
