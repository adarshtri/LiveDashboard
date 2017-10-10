import time
from kafka import SimpleProducer, KafkaClient
import numpy as np

kafka = KafkaClient('localhost:9092')
producer = SimpleProducer(kafka)

# assign a topic
topic = 'live-graph'

#while True:
#	inp = input('Enter some number:')
#	if inp=='exit':
#		producer.send_messages(topic,inp.encode('utf-8'))
#		exit(0)
#	producer.send_messages(topic,inp.encode('utf-8'))
#	time.sleep(2)

x = np.random.randint(100,size=100)

while True:
	producer.send_messages(topic,str(np.random.randint(100)).encode('utf-8'))
	time.sleep(1)
