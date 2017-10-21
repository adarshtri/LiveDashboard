from bokeh.layouts import layout
from bokeh.plotting import figure
from bokeh.io import curdoc
from bokeh.models import ColumnDataSource
from bokeh.client import push_session
import numpy as np
import datetime
from bokeh.models import HoverTool

currency = ['INR','USD','YEN']
value1 = [10, 7, 9]

stock = ['SENSEX', 'NASDAQ', 'ZUKI']
value2 = [101, 90, 81]

oil_price = ['Iraq', 'Syria', 'USA']
value3 = [2500, 3750, 4275]

date = datetime.datetime.now()
time = [date]*720
value4 = [0]*720


source1 = ColumnDataSource(dict(currency=currency,value1=value1))
source2 = ColumnDataSource(dict(stock=stock,value2=value2))
source3 = ColumnDataSource(dict(x0=[0]*3,y0=oil_price,oil_price=oil_price,value3=value3))
source4 = ColumnDataSource(dict(time=time, value4=value4))


def update_data():
    global value1,value2,value3,value4,time,currency,stock,oil_price
    for i in range(3):
        value1[i] = np.random.randint(0,100)
        value2[i] = np.random.randint(0,100)
        value3[i] = np.random.randint(2400,4500)
    v1 = value4[-1]
    value4 = value4[1:]
    time = time[1:]
    date = datetime.datetime.now()
    time.append(date)
    value4.append(v1 + np.random.randint(0, 100))

    new_data1 = dict(currency=currency,value1=value1)
    new_data2 = dict(stock=stock,value2=value2)
    new_data3 = dict(x0=[0]*3,y0=oil_price,oil_price=oil_price,value3=value3)
    new_data4 = dict(time=time, value4=value4)
    print(new_data1,new_data2,new_data3)
    source1.stream(new_data1,3)
    source2.stream(new_data2,3)
    source3.stream(new_data3,3)
    source4.stream(new_data4,720)


hover = HoverTool(tooltips=[
    ("No. of calls", "$y"),

])

p1 = figure(x_range=currency, sizing_mode='fixed', title='Currency Monitor',
            toolbar_location=None, tools="")
p1.yaxis.major_label_orientation='vertical'
p1.vbar(source=source1,x='currency',top='value1',width=0.8)

p2 = figure(x_range=stock, sizing_mode='fixed', title='Stock Monitor',
            toolbar_location=None, tools='')
p2.yaxis.major_label_orientation='vertical'
p2.line(source=source2,x='stock',y='value2', line_width=0.8)
p2.circle(source=source2,x='stock',y='value2', line_width=2)

p3 = figure(x_range=[2400,4500], y_range=oil_price,title='Oil Prices',sizing_mode='fixed',
            toolbar_location=None, tools='')

p3.line(source=source3,x='value3',y='oil_price',line_width=1)
p3.circle(source=source3,x='value3',y='oil_price',line_width=10)
p3.segment(source=source3,x0='x0',y0='y0',x1='value3',y1='oil_price',line_width=1)

p4 = figure(sizing_mode='stretch_both', x_axis_type='datetime',
             toolbar_location=None, tools=[hover], title='Call Flow Live Data',)
p4.yaxis.major_label_orientation='vertical'

p4.line(source=source4, x='time', y='value4', line_width=2)

plot = layout([p4],[p1,p2,p3],sizing_mode='stretch_both')

session = push_session(curdoc())
curdoc().add_root(plot)
curdoc().add_periodic_callback(update_data, 5000)
session.show(plot)
session.loop_until_closed(suppress_warning='Stop the python execution.')



