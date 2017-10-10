from kafka import KafkaConsumer

consumer = KafkaConsumer('live-graph',group_id='view',bootstrap_servers=['0.0.0.0:9092'])

for msg in consumer:
	if msg.value.decode('utf-8')=='exit':
		exit(0)
	print(msg.value.decode('utf-8'))
