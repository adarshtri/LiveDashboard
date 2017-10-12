from bokeh.io import curdoc
from bokeh.models import ColumnDataSource
from bokeh.plotting import figure
from bokeh.client import push_session
import numpy as np

currency = ['INR', 'USD', 'YEN']
value = [10, 10, 10]

p = figure(x_range=currency, sizing_mode='stretch_both', title='Currency Monitor',
           toolbar_location=None, tools="", y_axis_location='right')


def update_data():
    global value, currency
    for i in range(3):
        value[i] += np.random.randint(0, 100)
    new_data = dict(currency=currency, value=value)
    print(new_data)
    source.stream(new_data, 3)


source = ColumnDataSource(dict(currency=currency, value=value))

p.vbar(source=source,x='currency', top='value', width=0.9)
p.xgrid.grid_line_color = None
p.y_range.start = 0

session = push_session(curdoc())
curdoc().add_root(p)
curdoc().add_periodic_callback(update_data,1000)
session.show(p)
session.loop_until_closed(suppress_warning='Stop the python execution.')