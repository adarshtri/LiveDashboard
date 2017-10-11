from bokeh.io import curdoc
from bokeh.models import ColumnDataSource
from bokeh.plotting import figure
from bokeh.client import push_session
import psutil

factors = ['CPU', 'Memory', 'Disk']
x = [0, 0, 0]


def update_data():
    global x, factors
    x[0] = psutil.cpu_percent(interval=None)
    x[1] = psutil.virtual_memory().percent
    x[2] = psutil.disk_usage('/').percent
    new_data1 = dict(x0=[0] * 3, y0=factors, x1=x, y1=factors, x2=x, y2=factors)
    source.stream(new_data1, 3)


source = ColumnDataSource(dict(x0=[0]*3, y0=factors, x1=x, y1=factors, x2=x, y2=factors))

fig1 = figure(title='Resource Usage', sizing_mode='stretch_both', tools='', toolbar_location=None, y_range=factors,
              x_range=[0, 100])
fig1.segment(source=source, x0='x0', y0='y0', x1='x1', y1='y1', line_width=2, line_color='green')
fig1.circle(source=source, x='x2', y='y2', size=15, fill_color='orange', line_color='green', line_width=3)

session = push_session(curdoc())
curdoc().add_root(fig1)
curdoc().add_periodic_callback(update_data, 2000)
session.show(fig1)
session.loop_until_closed()
