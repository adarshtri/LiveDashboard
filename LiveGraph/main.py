`from bokeh.io import curdoc
from bokeh.models import ColumnDataSource
from bokeh.plotting import figure
import numpy as np
from kafka import KafkaConsumer
from bokeh.client import push_session




ct = 0
def update_data():
	global ct
	for msg in consumer:
		ct+=1
		try:
			int(msg.value.decode('utf-8'))
			#print(msg.value.decode('utf-8'))
			new_data = dict(x=[ct],y=[int(msg.value.decode('utf-8'))])
			source.stream(new_data,20)
		except:
			print("Wrong Input")

#curdoc().add_root(fig)

consumer = KafkaConsumer('live-graph',group_id='view',bootstrap_servers=['0.0.0.0:9092'])
source =  ColumnDataSource(dict(x=[],y=[]))

fig = figure(sizing_mode='stretch_both')
fig.circle(source=source,x='x',y='y',alpha=1,color='navy')
fig.line(source=source, x='x',y='y',alpha=0.85,color='red',line_width=2)

session = push_session(curdoc())
curdoc().add_root(fig)
curdoc().add_periodic_callback(update_data,20)
session.show(fig)
session.loop_until_closed()
