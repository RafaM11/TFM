import dash_dangerously_set_inner_html
import plotly.graph_objects as go
from dash import html
from dash import dcc
from dash import dash_table
import dash_daq as daq
import dash_bootstrap_components as dbc

empty_figure = go.Figure()

from utils.db_utils import get_all_boroughs, get_all_link_ids
from utils.plot_utils import display_traffic_map

boroughs_list = get_all_boroughs()
map = display_traffic_map()

layout_nav_bar = html.Div(children = [
    html.Div(children = [dash_dangerously_set_inner_html.DangerouslySetInnerHTML(
        '''
        <nav class="navbar navbar-expand-lg navbar-light bg-light">
            <div class="container-fluid">
                <a class="navbar-brand" href="/home">
                    <img src="/assets/logo_master.png" alt="" width="150" height="50" class="d-inline-block align-text-top">
                </a>

                <button class="navbar-toggler" type="button" data-bs-toggle="collapse" data-bs-target="#navbarSupportedContent" aria-controls="navbarSupportedContent" aria-expanded="false" aria-label="Toggle navigation">
                    <span class="navbar-toggler-icon"></span>
                </button>

                <div class="collapse navbar-collapse" id="navbarSupportedContent">
                    <ul class="navbar-nav ms-auto mb-2 mb-lg-0">
                        <li class="nav-item">
                            <a class="nav-link m-2 menu item" href="/home">Traffic Map</a>
                        </li>
                        <li class="nav-item">
                            <a class="nav-link m-2 menu item" href="/ml">Machine Learning</a>
                        </li>
                    </ul>
                </div>
            </div>
        </nav>
        '''
    )], id = 'nav-bar')
])

layout_map = html.Div(children = [
    html.Div(className = 'container', children = [], style = {'height': '1vh'}),

    html.Div(className = 'container', children = [
        html.Div(className = 'row', children = [
            html.Div(className = 'col-2', children = [html.H4(children = 'Real Time Map')])
        ]), 
        html.Div(className = 'row', children = [

            html.Div(className = 'col-3', children = [
                dbc.Card(children = [
                    dbc.CardHeader('Min. Speed'), 
                    dbc.CardBody([html.H5(id = 'min-speed-value', children = [], className = 'card-title'), html.P(id = 'min-speed-info', children = [''], className = 'card-text')])
                ], color = 'light', outline = True)
            ]), 

            html.Div(className = 'col-1', children = []), 

            html.Div(className = 'col-4', children = [
                dbc.Card(children = [
                    dbc.CardHeader('Avg. Speed'), 
                    dbc.CardBody([html.H5(id = 'avg-speed-value', children = [], className = 'card-title'), html.P(id = 'avg-speed-info', children = [''], className = 'card-text')])
                ], color = 'light', outline = False)
            ]), 

            html.Div(className = 'col-1', children = []), 

            html.Div(className = 'col-3', children = [ 
                dbc.Card(children = [
                    dbc.CardHeader('Max. Speed'), 
                    dbc.CardBody([html.H5(id = 'max-speed-value', children = [], className = 'card-title'), html.P(id = 'max-speed-info', children = [''], className = 'card-text')])
                ], color = 'light', outline = True)
            ]), 
        ])
    ]), 

    html.Div(className = 'container', children = [], style = {'height': '3vh'}),

    html.Div(className = 'container', children = [
        html.Div(className = 'row', children = [
            html.Div(className = 'col-3', children = [
                dbc.Label('Filter by borough:', html_for = 'borough-filtering'), 
                dcc.Dropdown(id = 'borough-filtering', options = [{'label': borough, 'value': borough} for borough in get_all_boroughs()], value = None, placeholder = 'None selects all boroughs')
            ]), 
            html.Div(className = 'col-3', children = [
                dbc.Label('Filter by traffic:', html_for = 'traffic-filtering'), 
                dcc.Dropdown(id = 'traffic-filtering', options = [{'label': i, 'value': i} for i in ['Heavy traffic', 'Very heavy traffic', 'Fluid traffic']], value = None, placeholder = 'None selects all traffic')
            ]), 

            html.Div(className = 'col-6', children = [
                dbc.Label('Filter historical data by days ago:', html_for = 'period-ago'), 
                dcc.Slider(
                    id = 'period-ago', 
                    included = False, 
                    min = 7, 
                    marks = {
                            7: {'label': '1W'}, 
                            14: {'label': '2W'}, 
                            21: {'label': '3W'}, 
                            30: {'label': '1M'}, 
                            60: {'label': '2M'}, 
                            }, 
                    max = 60,
                    step = None, 
                    value = 14
                )
            ]),
        ]), 
        html.Div(className = 'row', children = [
            html.Div(children = [html.Hr()]),
        ])
    ]), 

    html.Div(className = 'container', children = [
        html.Div(className = 'row', children = [
            dcc.Graph(id = 'traffic-map', config = {'displayModeBar': False}, figure = map, style = {'width': '100vw', 'height': '100vh'})
        ]),
    ]),

    html.Div(children = [], style = {'width': '100%', 'height': '2.5vh'}), 

    html.Div(className = 'container-fluid py-4 overflow-hidden', children = [

        html.Div(className = 'row gx-0 gy-0', children = [

            html.Div(className = 'col-4', children = []), 
            html.Div(className = 'col-4', children = [
                html.A(children = [
                    html.Img(src = '/assets/uma_logo.png', className = 'rounded mx-auto d-block')
                ], href = 'https://www.uma.es', target = 'black')
            ]), 
            html.Div(className = 'col-4', children = []), 
        ])

    ], style = {'width':'100%', 'height':'11vh', 'background-color': 'rgb(238, 240, 242)'}),

    dcc.Store(id = 'traffic-info'), 
    dcc.Interval(id = '1-sec-freq-interval', n_intervals = 0, interval = 1 * 1000), 
    dcc.Interval(id = 'reload-traffic-map-freq', n_intervals = 0, interval = 60 * 1000)

])

layout_ml = html.Div(children = [
    html.Div(children = [], style = {'width': '100%', 'height': '1.5vh'}), 

    html.Div(className = 'row', children = [
        html.Div(className = 'col-1', children = []), 
        html.Div(className = 'col-2', children = [html.H4(children = 'Predictive Model')])
    ]), 

    html.Div(className = 'container-fluid', children = [
        html.Div(className = 'row', children = [

            html.Div(className = 'col-1', children = []), 

            html.Div(className = 'col-2', children = [
                dbc.Label('Select link ID:', html_for = 'link-id-dropdown'), 
                dcc.Dropdown(id = 'link-id-dropdown', options = [{'label': {1: '2th Av. to Ganservoort St. on 11th Av.', 365: 'Borden Av. to Murray Hill neighborhood'}[id_], 'value': id_} for id_ in get_all_link_ids() if id_ in [1, 365]], value = None)
            ]), 

            #html.Div(className = 'col-1', children = []), 

            html.Div(className = 'col-2', children = [
                dbc.Label('Select total hours to predict:', html_for = 'forecast-hours'), 
                dcc.Slider(
                    id = 'forecast-hours', 
                    included = False, 
                    min = 8, 
                    marks = {
                            8: {'label': '8h'}, 
                            12: {'label': '12h'}, 
                            24: {'label': '1d'}, 
                            48: {'label': '2d'}, 
                            72: {'label': '3d'}, 
                            }, 
                    max = 72,
                    step = None, 
                    value = 24
                )
            ]),
            html.Div(className = 'col-7', children = []), 
        ]), 
    ]), 

    html.Div(className = 'row', children = [], style = {'height': '5vh'}), 

    html.Div(className = 'container-fluid', children = [
        html.Div(className = 'row', children = [
            html.Div(className = 'col-1', children = []), 
            html.Div(className = 'col-5', children = [
                dcc.Graph(id = 'time-series', figure = dict(layout = dict(paper_bgcolor = 'rgba(255, 0, 0, 0)', plot_bgcolor = 'rgba(200, 200, 200, 1)', xaxis = dict(visible = False, zeroline = False, showgrid = False), yaxis = dict(visible = False, zeroline = False, showgrid = False))), config = dict(displayModeBar = False))
            ]), 
            html.Div(className = 'col-5', children = [
                dcc.Graph(id = 'map-ml-selected-link-id', figure = dict(layout = dict(paper_bgcolor = 'rgba(0, 0, 0, 0)', plot_bgcolor = 'rgba(0, 0, 0, 0)', xaxis = dict(visible = False, zeroline = False, showgrid = False), yaxis = dict(visible = False, zeroline = False, showgrid = False))), config = dict(displayModeBar = False))
            ]), 
            html.Div(className = 'col-1', children = []), 
        ])
    ]),

    html.Div(children = [], style = {'width': '100%', 'height': '2.5vh'}), 

    html.Div(className = 'row', children = [
        html.Div(className = 'col-1', children = []), 
        html.Div(className = 'col-10', children = [html.Hr()]), 
        html.Div(className = 'col-1', children = []), 
    ]), 

    html.Div(children = [], style = {'width': '100%', 'height': '2.5vh'}), 

    html.Div(id = 'historical-table-div', children = []), 

    html.Div(className = 'container-fluid py-4 overflow-hidden', children = [

        html.Div(className = 'row gx-0 gy-0', children = [

            html.Div(className = 'col-4', children = []), 
            html.Div(className = 'col-4', children = [
                html.A(children = [
                    html.Img(src = '/assets/uma_logo.png', className = 'rounded mx-auto d-block')
                ], href = 'https://www.uma.es', target = 'black')
            ]), 
            html.Div(className = 'col-4', children = []), 
        ])

    ], style = {'width':'100%', 'height':'11vh', 'background-color': 'rgb(238, 240, 242)'}),

    dcc.Store(id = 'historical-table-data')

])