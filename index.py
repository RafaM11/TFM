from app import app
from dash import dcc
from dash import html
from dash.dependencies import Input, Output
from layouts import (layout_nav_bar, layout_map, layout_ml)

import callbacks

# Settings
debug = True

# Layout of the overall application
app.layout = html.Div(children = [
                        layout_nav_bar, 
                        dcc.Location(id = 'url', refresh = False), 
                        html.Div(id = 'page-content', children = [])
                        ], 
                    style = {'overflow-x': 'hidden', 'overflow-y': 'hidden'})

# Displays corresponding page to the Input URL pathname
@app.callback(
    Output('page-content', 'children'), 
    Input('url', 'pathname')
)
def display_page(pathname):
    if pathname == '/map':
        return layout_map
    elif pathname == '/ml':
        return layout_ml
    else:
        return layout_map

if __name__ == '__main__':
    app.run_server(debug = debug)