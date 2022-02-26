import os
import polyline
from configparser import ConfigParser
from geopy.distance import geodesic

import pandas as pd
import numpy as np
import dateutil
from datetime import timedelta

currentdir = os.path.abspath(os.path.dirname(__file__))
credentials_path = currentdir + '\\' + 'credentials.ini'

def config(filename = credentials_path, section = None): # Credentials file is not uploaded to the repository!
    parser = ConfigParser()
    parser.read(filename)
    db = {}
    if parser.has_section(section = section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} not found in {filename} file.')
    return db

def get_distance_from_polyline(x):
    points = polyline.decode(x)
    highway = []
    for i in range(len(points) - 1):
        highway.append(geodesic(points[i], points[i + 1]).miles)

    return points, sum(highway)

def game_in_madison_square_garden(date, model_dates):
    # import urllib.request, json 
    # with urllib.request.urlopen('https://es.global.nba.com/stats2/team/schedule.json?countryCode=ES&locale=es&teamCode=knicks') as url:
    #     data = json.loads(url.read().decode())
        
    # home_dates = []
        
    # for month in range(len(data['payload']['monthGroups'])):
    #     for match in range(len(data['payload']['monthGroups'][month]['games'])):
    #         if data['payload']['monthGroups'][month]['games'][match]['homeTeam']['profile']['abbr'] == 'NYK':
    #             home_dates.append(data['payload']['monthGroups'][month]['games'][match]['profile']['dateTimeEt'])

    # home_dates = [dateutil.parser.isoparse(x).replace(minute = 0).isoformat() if dateutil.parser.isoparse(x).minute == 30 else dateutil.parser.isoparse(x).isoformat() for x in home_dates] # Check that datetimes follow datetime format.
    # parsed_dates = [dateutil.parser.isoparse(x) for x in home_dates]
    # previous_hour = [(x - timedelta(hours = 1)).isoformat() for x in parsed_dates]
    # model_dates = home_dates + previous_hour
    if date in [model_dates]:
        return 1
    else:
        return 0

def is_weekend(ds):
    date = pd.to_datetime(ds)
    if date.weekday() > 4:
        return 1
    else:
        return 0
    

def mda(actual: np.ndarray, predicted: np.ndarray):
    return np.mean((np.sign(actual[1:] - actual[:-1]) == np.sign(predicted[1:] - predicted[:-1])).astype(int))

def rmse(actual: np.ndarray, predicted: np.ndarray):
    length = len(actual)
    return np.sqrt((sum((actual - predicted) ** 2)) / length)

def is_weekend(ds):
    date = pd.to_datetime(ds)
    if date.weekday() > 4:
        return 1
    else:
        return 0
