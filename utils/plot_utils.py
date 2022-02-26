import plotly.graph_objects as go
import numpy as np

from utils.db_utils import get_all_boroughs, get_all_link_ids, get_coordinates_from_link_id, get_centroid_from_borough
from utils.params_utils import default_coordinates
from utils.misc_utils import config

def get_color_from_traffic_density(traffic: str) -> str:
    traffic_density_dict = {'Heavy traffic': 'rgb(45, 125, 210)', 'Very heavy traffic': 'rgb(244, 93, 1)', 'Fluid traffic': 'rgb(151, 204, 4)'}
    return traffic_density_dict[traffic]

def get_line_width_and_marker_size_from_color(color: str) -> int:
    if color == 'grey':
        return 1.5, 5
    else:
        return 3.5, 10

def display_traffic_map(plot_title: str = None, borough: str = None, plot_dict: dict = None) -> go.Figure():
    fig = go.Figure()

    list_of_link_ids = get_all_link_ids()

    for link_id in list_of_link_ids:

        if plot_dict is None:
            color_link = 'grey'
        else:
            if link_id not in list(plot_dict.keys()):
                color_link = 'grey'
            else:
                if plot_dict[link_id]['traffic'] is None:
                    color_link = 'grey'
                else:
                    color_link = get_color_from_traffic_density(plot_dict[link_id]['traffic'])

        coordinates_df = get_coordinates_from_link_id(link_id = link_id)
        fig.add_trace(go.Scattermapbox( 
            lat = [x for x in coordinates_df['latitude']], 
            lon = [x for x in coordinates_df['longitude']], 
            mode = 'markers+lines', 
            line = go.scattermapbox.Line(color = color_link, width = get_line_width_and_marker_size_from_color(color_link)[0]), 
            marker = go.scattermapbox.Marker(color = color_link, size = get_line_width_and_marker_size_from_color(color_link)[1]), 
            name = f'Link ID: {link_id}', 
            showlegend = False))

    token = config(section = 'mapbox')

    fig.update_layout(
        uirevision = 'constant', # Set whatever string value to not losing previous zoom.
        hovermode = 'closest', 
        mapbox = dict(
                    accesstoken = token['mapbox_access_token'], 
                    bearing = 0, 
                    center = go.layout.mapbox.Center(
                        lat = default_coordinates['lat'] if borough is None else get_centroid_from_borough(borough)['lat'], 
                        lon = default_coordinates['lon'] if borough is None else get_centroid_from_borough(borough)['lon']),
                    pitch = 0,
                    style = 'basic', 
                    zoom = 12
                    ), 
        margin = dict(l = 0, r = 0, t = 50),
        title = {'text': 'Last observation at' + ' ' + plot_title if plot_title is not None else ''}, 
    )

    fig.update_layout(
        updatemenus = [
            dict(
                bgcolor = 'gray', 
                bordercolor = 'rgba(0, 0, 0, 0.1)', 
                buttons = [
                    dict(
                        args = [
                            {
                            'mapbox.center.lat': get_centroid_from_borough(borough_btn)['lat'], 
                            'mapbox.center.lon': get_centroid_from_borough(borough_btn)['lon']
                            }], 
                        label = borough_btn, 
                        method = 'relayout'
                        ) for borough_btn in get_all_boroughs()], 
                direction = 'right', 
                font = {'color': 'rgb(0, 0, 0)', 'size': 8.5}, 
                pad = {'l': 0, 't': 0}, 
                showactive = False,
                type = 'buttons', 
                x = 1, 
                xanchor = 'right', 
                y = 1, 
                yanchor = 'top'
            )
        ]
    )

    return fig

def display_historical_map(link_id: int) -> go.Figure():
    fig = go.Figure()

    coordinates_df = get_coordinates_from_link_id(link_id = link_id)

    fig.add_trace(go.Scattermapbox( 
        lat = [x for x in coordinates_df['latitude']], 
        lon = [x for x in coordinates_df['longitude']], 
        mode = 'markers+lines', 
        line = go.scattermapbox.Line(color = 'darkblue', width = 5), 
        marker = go.scattermapbox.Marker(color = 'darkblue', size = 10), 
        name = f'Link ID: {link_id}', 
        showlegend = False))

    token = config(section = 'mapbox')

    fig.update_layout(
        uirevision = 'constant', # Set whatever string value to not losing previous zoom.
        hovermode = 'closest', 
        mapbox = dict(
                    accesstoken = token['mapbox_access_token'], 
                    bearing = 0, 
                    pitch = 0,
                    center = go.layout.mapbox.Center(
                        lat = np.mean(coordinates_df['latitude']), 
                        lon = np.mean(coordinates_df['longitude']),
                    ),  
                    style = 'streets', 
                    zoom = 14
                    ), 
        margin = dict(l = 0, r = 0, t = 0, b = 0),
    )

    return fig