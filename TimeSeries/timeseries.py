import datetime
from bokeh.models import ColumnDataSource
import numpy as np
from bokeh.plotting import figure
from bokeh.client import push_session
from bokeh.io import curdoc

date = datetime.datetime.now()
time = [date]*3600
value = [0]*3600

source = ColumnDataSource(dict(time=time, value=value))


def update_data():
    global time, value
    v1 = value[-1]
    value = value[1:]
    time = time[1:]
    date = datetime.datetime.now()
    time.append(date)
    value.append(v1+np.random.randint(0,5))
    new_data = dict(time=time,value=value)
    print(new_data)
    source.stream(new_data,3600)


fig = figure(sizing_mode='stretch_both', x_axis_type='datetime')

fig.line(source=source, x='time', y='value', line_width=2)


session = push_session(curdoc())
curdoc().add_root(fig)
curdoc().add_periodic_callback(update_data, 1000)
session.show(fig)
session.loop_until_closed(suppress_warning='Stop the python execution.')
