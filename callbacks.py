from os import link
from tracemalloc import start
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
from dash import dash_table
from dash import html
from dash import dcc
import dash_bootstrap_components as dbc

from app import app
from utils.db_utils import get_centroid_from_borough, get_real_time_traffic_data, get_avg_and_quantiles_speed_from_historical_data, get_speed_from_link_id
from utils.params_utils import default_coordinates
from utils.plot_utils import display_traffic_map, display_historical_map
from utils.misc_utils import game_in_madison_square_garden, rmse, mda, is_weekend
from layouts import empty_figure

from datetime import datetime, timedelta
import calendar
import pytz
import pandas as pd
import numpy as np
import json
import urllib.request
import dateutil
from ast import literal_eval

import plotly.graph_objects as go

from fbprophet.serialize import model_to_json, model_from_json

# @app.callback(
#     Output('nyc-time', 'value'), 
#     Input('1-sec-freq-interval', 'n_intervals')
# )
# def update_datetime_text(n_intervals):
#     nyc_timezone = 'US/Eastern'
#     return datetime.now(tz = pytz.timezone(nyc_timezone)).strftime('%H:%M:%S')

@app.callback(
    Output('traffic-info', 'data'), 
    [Input('reload-traffic-map-freq', 'n_intervals'),
     Input('period-ago', 'value')]
)
def update_traffic_map_links(n_intervals, days_ago):
    threshold_date = (datetime.now() - timedelta(days = days_ago)).strftime('%Y-%m-%d')
    weekday = datetime.today().weekday()
    parsed_weekday = weekday + 1 if weekday != 6 else 0
    filtered_hour = datetime.now().hour
    historical_speed = get_avg_and_quantiles_speed_from_historical_data(threshold_date, parsed_weekday, filtered_hour)
    real_time_speed = get_real_time_traffic_data()

    real_time_speed = real_time_speed.groupby(['link_id', 'borough'], as_index = False).agg({'measurement_date': 'max', 'speed': ['min', 'mean', 'median', 'max']})
    real_time_speed.columns = ['_'.join(col) if 'speed' in col else ''.join(col) for col in real_time_speed.columns]
    real_time_speed = real_time_speed.rename(columns = {'measurement_datemax': 'last_observation', 'speed': 'speed_median'})

    real_time_speed['traffic'] = None

    for link_id in real_time_speed['link_id']:
        real_time_median_speed_value = real_time_speed['speed_median'][real_time_speed['link_id'] == link_id].item()
        real_time_median_speed_index = real_time_speed['speed_median'][real_time_speed['link_id'] == link_id].index.item()
        
        filtered_historical_speed = historical_speed[historical_speed['link_id'] == link_id]
        if len(filtered_historical_speed) != 0:
            if (real_time_median_speed_value >= filtered_historical_speed['q1'].item() and real_time_median_speed_value <= filtered_historical_speed['q3'].item()):
                real_time_speed.at[real_time_median_speed_index, 'traffic'] = 'Heavy traffic'
            elif (real_time_median_speed_value < filtered_historical_speed['q1'].item()):
                real_time_speed.at[real_time_median_speed_index, 'traffic'] = 'Very heavy traffic'
            elif (real_time_median_speed_value > filtered_historical_speed['q3'].item()):
                real_time_speed.at[real_time_median_speed_index, 'traffic'] = 'Fluid traffic'

    return real_time_speed.to_dict(orient = 'records')

@app.callback(
    [Output('traffic-map', 'figure'), 
     Output('min-speed-value', 'children'), 
     Output('avg-speed-value', 'children'), 
     Output('max-speed-value', 'children'), 
     Output('min-speed-info', 'children'), 
     Output('avg-speed-info', 'children'), 
     Output('max-speed-info', 'children')], 
    [Input('traffic-info', 'data'), 
     Input('borough-filtering', 'value'), 
     Input('traffic-filtering', 'value')], 
)
def update_traffic_map(data, borough, traffic):
    if (data is None or len(data) == 0):
        raise PreventUpdate
    else:
        df = pd.DataFrame(data)
        if borough is not None and traffic is not None:
            df = df[(df['borough'] == borough) & (df['traffic'] == traffic)]
        elif borough is not None and traffic is None:
            df = df[(df['borough'] == borough)]
        elif borough is None and traffic is not None:
            df = df[(df['traffic'] == traffic)]
        #plot_title = max(df['last_observation']).to_pydatetime()

        df['speed_min'] = [np.round(1.609344 * x, 0) for x in df['speed_min']]
        df['speed_mean'] = [np.round(1.609344 * x, 0) for x in df['speed_mean']]
        df['speed_median'] = [np.round(1.609344 * x, 0) for x in df['speed_median']]
        df['speed_max'] = [np.round(1.609344 * x, 0) for x in df['speed_max']]

        borough_max_speed, borough_min_speed = df[df.index == df['speed_min'].idxmax()]['borough'].item(), df[df.index == df['speed_min'].idxmin()]['borough'].item()
        link_max_speed, link_min_speed = df[df.index == df['speed_min'].idxmax()]['link_id'].item(), df[df.index == df['speed_min'].idxmin()]['link_id'].item()

        plot_title = max(df['last_observation'])
        #localized_timestamp = pytz.timezone('US/Eastern').localize(plot_title)
        #plot_title = localized_timestamp.astimezone(pytz.timezone('Europe/Madrid')).replace(tzinfo = None).isoformat().replace('T', ' ')
        speed_unit = 'km/h'
        plot_dict = df.set_index('link_id').T.to_dict(orient = 'dict')
        return [display_traffic_map(plot_title, borough, plot_dict), 
                str(min(df['speed_min'])) + ' ' + speed_unit, 
                str(np.round(np.mean(df['speed_mean']), 0)) + ' ' + speed_unit,
                str(max(df['speed_max'])) + ' ' + speed_unit, 
                f'Taken in {borough_min_speed} (Link ID: {str(link_min_speed)})', 
                f'Average of {str(len(df))} observations', 
                f'Taken in {borough_max_speed} (Link ID: {str(link_max_speed)})']

@app.callback(
    [Output('time-series', 'figure'), 
     Output('map-ml-selected-link-id', 'figure')],
    [Input('link-id-dropdown', 'value'), 
     Input('forecast-hours', 'value')]
)
def update_predictions(link_id, hours_to_predict):
    if link_id is None:
        fig = go.Figure()
        fig.update_layout(margin = dict(t = 0, l = 0, r = 0, b = 0), paper_bgcolor = 'rgba(0, 0, 0, 0)', plot_bgcolor = 'rgba(0, 0, 0, 0)')
        fig.update_xaxes(showgrid = False, zeroline = False, visible = False)
        fig.update_yaxes(showgrid = False, zeroline = False, visible = False)

        empty_map = go.Figure()
        empty_map.update_layout(paper_bgcolor = 'rgba(0, 0, 0, 0)', plot_bgcolor = 'rgba(0, 0, 0, 0)', xaxis = dict(visible = False, zeroline = False, showgrid = False), yaxis = dict(visible = False, zeroline = False, showgrid = False))
        return fig, empty_map
    else:
        #nyc_datetime = datetime.now(tz = pytz.timezone('US/Eastern'))
        nyc_datetime = datetime(2022, 2, 17, 0, 0, 0)
        test_dict = get_speed_from_link_id(link_id = link_id, measurement_date = (nyc_datetime - timedelta(days = 3)).strftime('%Y-%m-%d'))
        if bool(test_dict):
            df = pd.DataFrame(test_dict)
            df = df.set_index('measurement_date')
            df.index = pd.to_datetime(df.index) 
            df = df.resample('1H').median()
            df = df.reset_index(drop = False)
            df = df.rename(columns = {'measurement_date': 'ds', 'speed': 'y'})
            df = df.dropna()

            with open(f'C://Users//rcmpo//OneDrive//Escritorio//TFM//application//models//{link_id}.json', 'r') as fin:
                m = model_from_json(json.load(fin))  # Load model
            future = m.make_future_dataframe(periods = 360, freq = 'H')

            with urllib.request.urlopen('https://es.global.nba.com/stats2/team/schedule.json?countryCode=ES&locale=es&teamCode=knicks') as url:
                data = json.loads(url.read().decode())
                
            home_dates = []
                
            for month in range(len(data['payload']['monthGroups'])):
                for match in range(len(data['payload']['monthGroups'][month]['games'])):
                    if data['payload']['monthGroups'][month]['games'][match]['homeTeam']['profile']['abbr'] == 'NYK':
                        home_dates.append(data['payload']['monthGroups'][month]['games'][match]['profile']['dateTimeEt'])

            home_dates = [dateutil.parser.isoparse(x).replace(minute = 0).isoformat() if dateutil.parser.isoparse(x).minute == 30 else dateutil.parser.isoparse(x).isoformat() for x in home_dates] # Check that datetimes follow datetime format.
            parsed_dates = [dateutil.parser.isoparse(x) for x in home_dates]
            previous_hour = [(x - timedelta(hours = 1)).isoformat() for x in parsed_dates]
            model_dates = home_dates + previous_hour
            
            future['weekend'] = future.apply(lambda x: is_weekend(x['ds']), axis = 1)
            future['knicks_game'] = future.apply(lambda x: game_in_madison_square_garden(x['ds'], model_dates), axis = 1)

            start_date, end_date = nyc_datetime, nyc_datetime + timedelta(hours = hours_to_predict)
            future = future[(future['ds'] >= start_date.strftime('%Y-%m-%d %H:%M:%S')) & (future['ds'] <= end_date.strftime('%Y-%m-%d %H:%M:%S'))]
            forecast = m.predict(future)

            dirty_df = pd.DataFrame(test_dict)

            fig = go.Figure()

            fig.add_trace(go.Scatter(x = dirty_df['measurement_date'], 
                                    y = dirty_df['speed'], 
                                    mode = 'markers', 
                                    name = 'All observations',
                                    marker = dict(size = 7, color = 'rgba(150, 190, 240, 0.8)')))

            fig.add_trace(go.Scatter(x = df.set_index('ds')['y'].index, 
                                    y = df.set_index('ds')['y'].values, 
                                    mode = 'lines+markers', 
                                    name = 'Real', 
                                    marker = dict(size = 11, color = 'rgba(63, 131, 192, 0.8)'), 
                                    line = dict(width = 2, color = 'rgba(63, 131, 192, 1)')))

            fig.add_trace(go.Scatter(x = forecast.set_index('ds')['yhat'].index, 
                                    y = forecast.set_index('ds')['yhat'].values, 
                                    mode = 'markers+lines', 
                                    name = 'Forecast', 
                                    marker = dict(size = 9, color = 'rgba(92, 113, 136, 0.8)'), 
                                    line = dict(width = 2, color = 'rgba(92, 113, 136, 1)')))

            fig.add_trace(go.Scatter(x = forecast.set_index('ds')['yhat_lower'].index, 
                                    y = forecast.set_index('ds')['yhat_lower'].values, 
                                    hoverinfo = 'skip', 
                                    mode = 'lines', 
                                    line = dict(width = 0),
                                    showlegend = False)),

            fig.add_trace(go.Scatter(x = forecast.set_index('ds')['yhat_upper'].index, 
                                    y = forecast.set_index('ds')['yhat_upper'].values, 
                                    hoverinfo = 'skip', 
                                    fillcolor = 'rgba(92, 113, 136, 0.3)', 
                                    fill = 'tonexty', 
                                    mode = 'lines', 
                                    line = dict(width = 0), 
                                    showlegend = False)), 

            fig.update_layout(margin = dict(t = 25, l = 0, r = 0, b = 0), paper_bgcolor = 'rgba(0, 0, 0, 0)', plot_bgcolor = 'rgba(255, 255, 255, 1)')
            fig.update_layout(xaxis_title = 'Datetime', yaxis_title = 'Speed (mph)')
            fig.update_layout(font = dict(size = 12))
        else:
            fig = go.Figure()
            fig.update_layout(title = f'There are no data in last 3 days for the selected highway.')
        return fig, display_historical_map(link_id = link_id)

@app.callback(
    [Output('historical-table-div', 'children'), 
     Output('historical-table-data', 'data')], 
    Input('link-id-dropdown', 'value')
)
def update_historical_datatable(link_id):
    if link_id is None:
        return [], {}
    else:
        hours = np.arange(0, 24)

        data = get_speed_from_link_id(link_id = link_id, measurement_date = '2021-01-01')
        df = pd.DataFrame(data)
        q3, q1 = np.quantile(df['speed'], 0.75), np.quantile(df['speed'], 0.25)
        iqr = q3 - q1
        upper_limit, lower_limit = q3 + 1.5 * iqr, q1 - 1.5 * iqr
        df = df[(df['speed'] > lower_limit) & (df['speed'] < upper_limit)]
        df = df.set_index('measurement_date')
        df.index = pd.to_datetime(df.index)
        df = df.sort_index(ascending = False)
        df = df.reset_index(drop = False)
        
        return [
            html.Div(className = 'row', children = [
                html.Div(className = 'col-1', children = []), 
                html.Div(className = 'col-4', children = [html.H4(children = 'Historical Analysis')])
            ]), 
            html.Div(className = 'container-fluid', children = [
                html.Div(className = 'row', children = [
                    html.Div(className = 'col-1', children = []), 
                    html.Div(className = 'col-1', children = [
                        dbc.Label('Select year:', html_for = 'select-year-dropdown'), 
                        dcc.Dropdown(id = 'select-year-dropdown', options = [{'label': year, 'value': year} for year in [2021, 2022]], value = None, placeholder = 'None selects all years')
                    ]), 
                    html.Div(className = 'col-1', children = [
                        dbc.Label('Select month:', html_for = 'select-month-dropdown'), 
                        dcc.Dropdown(id = 'select-month-dropdown', options = [{'label': month, 'value': month} for month in list(calendar.month_name)[1:]], value = None, placeholder = 'None selects all months')
                    ]), 
                    html.Div(className = 'col-1', children = [
                        dbc.Label('Select hour:', html_for = 'select-hour-dropdown'), 
                        dcc.Dropdown(id = 'select-hour-dropdown', 
                                     options = [{'label': str(((i), (i + 1) % len(hours))[0]) + ':00 to ' + str(((i), (i + 1) % len(hours))[1]) + ':00', 
                                                 'value': str(((i), (i + 1) % len(hours)))} for i in range(len(hours))], 
                                     value = None, 
                                     placeholder = 'None selects all hours')
                    ]), 
                    html.Div(className = 'col-6', children = [])
                ]), 

                html.Div(className = 'row', children = [], style = {'height': '5vh'}), 

                html.Div(className = 'row', children = [
                    html.Div(className = 'col-1', children = []), 
                    html.Div(className = 'col-4', children = [
                        dash_table.DataTable(
                            id = 'historical-info-table', 
                            columns = [{'name': col.replace('_', ' ').title() + ' (mph) ', 'id': col} if col == 'speed' else {'name': col.replace('_', ' ').title() + ' (GMT-5) ', 'id': col} for col in df.columns], 
                            data = df.to_dict(orient = 'records'), 
                            page_size = 15, 
                            style_as_list_view = True, 
                            style_cell = {'textAlign': 'center', 'fontSize': 15, 'font-family': 'Helvetica', 'backgroundColor': 'rgb(255, 255, 255)', 'border': '1px solid rgb(230, 230, 230)'},
                            style_header = {'fontWeight': 'bold', 'fontSize': 16, 'font-family': 'Helvetica', 'backgroundColor': 'rgb(250, 250, 250)', 'border': '1px solid rgb(230, 230, 230)'})
                    ]), 
                    html.Div(className = 'col-6', children = [dcc.Graph(id = 'historical-info-graph', figure = {})]),
                    html.Div(className = 'col-1', children = [])
                ])
            ])
        ], df.to_dict(orient = 'records')

@app.callback(
    [Output('historical-info-graph', 'figure'), 
     Output('historical-info-table', 'data')], 
    [Input('select-year-dropdown', 'value'), 
     Input('select-month-dropdown', 'value'), 
     Input('select-hour-dropdown', 'value')], 
    State('historical-table-data', 'data'), 
)
def update_table_and_month(year, month, hour, data):
    df = pd.DataFrame(data)
    if year is None:
        if month is None:
            if hour is None:
                pass
            else:
                hour = literal_eval(hour)
                start_date, end_date = hour[0], hour[1]
                df['hour'] = [datetime.fromisoformat(x).hour for x in df['measurement_date']]
                df = df[(df['hour'] >= start_date) & (df['hour'] < end_date)]
                df = df.drop(columns = 'hour')
        else:
            if hour is None:
                df['month'] = [calendar.month_name[datetime.fromisoformat(x).month] for x in df['measurement_date']]
                df = df[(df['month'] == month)]
                df = df.drop(columns = ['month'])
            else: 
                hour = literal_eval(hour)
                start_date, end_date = hour[0], hour[1]
                df['hour'] = [datetime.fromisoformat(x).hour for x in df['measurement_date']]
                df['month'] = [calendar.month_name[datetime.fromisoformat(x).month] for x in df['measurement_date']]
                df = df[(df['month'] == month) & (df['hour'] >= start_date) & (df['hour'] < end_date)]
                df = df.drop(columns = ['month', 'hour'])
    else:
        if month is None:
            if hour is None:
                df['year'] = [datetime.fromisoformat(x).year for x in df['measurement_date']]
                df = df[df['year'] == year]
                df = df.drop(columns = 'year')
            else:
                hour = literal_eval(hour)
                start_date, end_date = hour[0], hour[1]
                df['hour'] = [datetime.fromisoformat(x).hour for x in df['measurement_date']]
                df['year'] = [datetime.fromisoformat(x).year for x in df['measurement_date']]
                df = df[(df['year'] == year) & (df['hour'] >= start_date) & (df['hour'] < end_date)]
                df = df.drop(columns = ['year', 'hour'])
        else:
            if hour is None:
                date = datetime(year, {name:num for num, name in enumerate(list(calendar.month_name)[1:], 1)}[month], 1)
                if date > datetime.now():
                    df = pd.DataFrame()
                else:
                    df['year'] = [datetime.fromisoformat(x).year for x in df['measurement_date']]
                    df['month'] = [calendar.month_name[datetime.fromisoformat(x).month] for x in df['measurement_date']]
                    df = df[(df['year'] == year) & (df['month'] == month)]
                    df = df.drop(columns = ['year', 'month'])
            else:
                date = datetime(year, {name:num for num, name in enumerate(list(calendar.month_name)[1:], 1)}[month], 1)
                if date > datetime.now():
                    df = pd.DataFrame()
                else:
                    hour = literal_eval(hour)
                    start_date, end_date = hour[0], hour[1]
                    df['hour'] = [datetime.fromisoformat(x).hour for x in df['measurement_date']]
                    df['year'] = [datetime.fromisoformat(x).year for x in df['measurement_date']]
                    df['month'] = [calendar.month_name[datetime.fromisoformat(x).month] for x in df['measurement_date']]
                    df = df[(df['year'] == year) & (df['month'] == month) & (df['hour'] >= start_date) & (df['hour'] < end_date)]
                    df = df.drop(columns = ['year', 'month', 'hour'])

    print(df)
    fig = go.Figure()
    if not df.empty:
        fig.add_trace(go.Histogram(x = df['speed']))
        fig.update_layout(margin = dict(t = 0, l = 0, r = 0, b = 0))
    else:
        fig.update_layout(title = 'There are no data for the selected period.')
        fig.update_layout(paper_bgcolor = 'rgba(0, 0, 0, 0)', plot_bgcolor = 'rgba(0, 0, 0, 0)', xaxis = dict(visible = False, zeroline = False, showgrid = False), yaxis = dict(visible = False, zeroline = False, showgrid = False))

    return fig, df.to_dict(orient = 'records')