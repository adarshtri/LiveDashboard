from bokeh.io import curdoc
from bokeh.models import ColumnDataSource
from bokeh.plotting import figure
from bokeh.client import push_session
from bokeh.embed import server_session, components
import numpy as np
import json

currency = ['INR', 'USD', 'YEN']
value = [10, 10, 10]

p = figure(x_range=currency, sizing_mode='stretch_both', title='Currency Monitor',
           toolbar_location=None, tools="", y_axis_location='right')


def update_data():
    global value, currency
    for i in range(3):
        value[i] += np.random.randint(0, 5)
    new_data = dict(currency=currency, value=value)
    print(new_data)
    source.stream(new_data, 3)


source = ColumnDataSource(dict(currency=currency, value=value))

p.vbar(source=source,x='currency', top='value', width=0.6)
p.xgrid.grid_line_color = None
p.y_range.start = 0

session = push_session(curdoc())
curdoc().add_root(p)
curdoc().add_periodic_callback(update_data,1000)

script = server_session(p, session_id=session.id)


sc, div = components(p)

print(div)

string_before = '''<!DOCTYPE html>
<html lang="en">
    <head>
        <meta charset="utf-8">
        <title>Bokeh Scatter Plots</title>

        <link rel="stylesheet" href="http://cdn.pydata.org/bokeh/release/bokeh-0.12.6.min.css" type="text/css" />
        <script type="text/javascript" src="http://cdn.pydata.org/bokeh/release/bokeh-0.12.6.min.js"></script>
'''

string_middle = '''</head>
    <body>'''

string_after = '''</body>
</html>'''

html = open('live-graph.html', 'w')
html.write(string_before+"\n"+script+string_middle+"\n"+div+"\n"+string_after)
html.close()

session.loop_until_closed(suppress_warning='Stop the python execution.')

