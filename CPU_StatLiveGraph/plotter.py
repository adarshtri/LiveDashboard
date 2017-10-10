from bokeh.io import curdoc
from bokeh.models import ColumnDataSource
from bokeh.plotting import figure
from bokeh.client import push_session

import psutil

factors = ['CPU','Memory','Disk']
x = [0,0,0]

#ct=0

#def update_data():
#    global ct
#    ct+=1
#    new_data = dict(x=[ct],y=[psutil.cpu_percent(interval=None)])
#    source.stream(new_data,20)

def update_data():
    global x,factors
    x1 = []
    x1.append(psutil.cpu_percent(interval=None))
    mem = psutil.virtual_memory()
    print(mem)
    x1.append(mem.percent)
    dis = psutil.disk_usage('/')
    x1.append(dis.percent)
    x = x1[:]
    factors1=['CPU','Memory','Disk']
    factors=factors1[:]
    print(x)
    print(factors)
    new_data1 = dict(x0=[0]*3, y0=factors, x1=x, y1=factors,x2=x,y2=factors)
    print("source1")
    print(new_data1)
    source1.stream(new_data1,3)


source1 = ColumnDataSource(dict(x0=[0]*3, y0=factors, x1=x, y1=factors,x2=x,y2=factors))
#source2 = ColumnDataSource(dict(x=x,y=factors))

#fig = figure(sizing_mode='stretch_both')
#fig.circle(source=source,x='x',y='y',alpha=1,color='navy')
#fig.line(source=source, x='x',y='y',alpha=0.5,color='red',line_width=2)

fig1 = figure(title='Dot Categorical Plot',sizing_mode='stretch_both',tools='',toolbar_location=None,y_range=factors,x_range=[0,100])
fig1.segment(source=source1,x0='x0',y0='y0',x1='x1',y1='y1',line_width=2,line_color='green')
fig1.circle(source=source1,x='x2',y='y2',size=15,fill_color='orange', line_color='green', line_width=3)

session = push_session(curdoc())
curdoc().add_root(fig1)
curdoc().add_periodic_callback(update_data,2000)
session.show(fig1)
session.loop_until_closed()