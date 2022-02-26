import psycopg2
import pandas as pd
import numpy as np
from sodapy import Socrata

try:
    from utils.misc_utils import config, get_distance_from_polyline
except ModuleNotFoundError:
    from misc_utils import config, get_distance_from_polyline

class DBConnection():

    def __init__(self):
        pass

    def connect(self):
        params = config(section = 'postgresql')
        # We do not need PostgreSQL URL database when using psycopg2 module.
        params = {key:value for key, value in params.items() if key != 'url'}
        conn = None
        try:
            print('Performing connection with database...')
            conn = psycopg2.connect(**params)
            print('Connection successful !')
        except (Exception, psycopg2.DatabaseError) as e:
            print(e)
        return conn

    def perform_query(self, query: str) -> pd.DataFrame:
        conn = self.connect()
        df = pd.DataFrame()
        try:
            print('Performing query to database...')
            df = pd.read_sql_query(sql = query, con = conn)
            print('Query finished !')
        except Exception as e:
            print(e)
        finally:
            conn.close()
            print('Connection closed.')
        return df

def get_all_link_ids() -> list:
    db = DBConnection()
    query = 'SELECT DISTINCT link_id FROM tb_links'
    id_links_df = db.perform_query(query)
    id_links_list = list(id_links_df['link_id'])
    return id_links_list

def get_all_boroughs() -> list:
    db = DBConnection()
    query = 'SELECT DISTINCT borough FROM tb_links'
    boroughs_df = db.perform_query(query)
    boroughs_list = list(boroughs_df['borough'])
    return sorted(boroughs_list)

def get_coordinates_from_link_id(link_id: int) -> pd.DataFrame:
    db = DBConnection()
    coordinates_df = db.perform_query(f'SELECT latitude, longitude FROM tb_links, UNNEST(points) WHERE link_id = {link_id}')
    return coordinates_df

def get_borough_from_link_id(link_id: int) -> str:
    db = DBConnection()
    df = db.perform_query(f'SELECT borough FROM tb_links WHERE link_id = {link_id}')
    if len(df) == 0:
        borough = None
        print(f'Impossible to find ID {link_id} in database.')
    else:
        borough = df['borough'].item()
    return borough

def get_centroid_from_borough(borough: str) -> dict:
    db = DBConnection()
    coordinates_df = db.perform_query(f"SELECT latitude, longitude FROM tb_links, UNNEST(points) WHERE borough = '{borough}'")
    lat, lon = np.mean(coordinates_df['latitude']), np.mean(coordinates_df['longitude'])
    centroid = dict(lat = lat, lon = lon)
    return centroid

def get_most_recent_date_stored_for_each_link_id() -> pd.DataFrame:
    db = DBConnection()
    query = 'SELECT link_id, MAX(measurement_date) AS most_recent_date FROM tb_sensors GROUP BY link_id'
    most_recent_date_per_id_df = db.perform_query(query)
    return most_recent_date_per_id_df

def get_avg_and_quantiles_speed_from_historical_data(threshold_date: str, weekday: int, hour: int) -> pd.DataFrame:
    db = DBConnection()
    query = f"""SELECT link_id, AVG(speed) AS avg_speed, PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY speed) AS q1, PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY speed) AS q2, PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY SPEED) AS q3
                FROM tb_historical_traffic
                WHERE measurement_date > '{threshold_date}'
                AND EXTRACT(dow FROM measurement_date) = {weekday}
                AND EXTRACT(hour FROM measurement_date) = {hour}
                GROUP BY link_id;"""
    speed_info_df = db.perform_query(query)
    return speed_info_df

def get_speed_from_link_id(link_id: int, measurement_date: str) -> dict:
    db = DBConnection()
    query = f"""SELECT speed, measurement_date
                FROM tb_historical_traffic
                WHERE link_id = {link_id}
                AND measurement_date > '{measurement_date}'"""
    speed_and_date = db.perform_query(query)
    return speed_and_date.to_dict(orient = 'records')

def get_real_time_traffic_data() -> pd.DataFrame:
    db = DBConnection()
    query = "SELECT id_measurement, tb_real_time_traffic.link_id, speed, travel_time, measurement_date, borough \
             FROM tb_real_time_traffic \
             JOIN tb_links ON tb_real_time_traffic.link_id = tb_links.link_id"
    real_time_traffic_df = db.perform_query(query)
    return real_time_traffic_df

class SocrataAPI():

    def __init__(self, dataset_identifier, content_type):
        self.dataset_identifier = dataset_identifier
        self.content_type = content_type

    def connect(self):
        params = config(section = 'socrata')
        timeout = 600
        client = None
        try:
            print('Performing connection with Socrata API...')
            client = Socrata(domain = params['domain'], 
                             app_token = params['app_token'], 
                             username = params['username'], 
                             password = params['password'], 
                             timeout = timeout)
            print('Connection successful !')
        except Exception as e:
            print(e)
        return client

    def perform_query(self, query: str) -> list:
        client = self.connect()
        response = []
        try:
            print('Performing query to Socrata API...')
            response = client.get(dataset_identifier = self.dataset_identifier,
                                  content_type = self.content_type, 
                                  query = query)
            print('Query finished !')
        except Exception as e:
            print(e)
        finally:
            client.close()
            print('Connection closed.')
        return response

# Control if there are data in the database or the inserting flow is started at the moment.
def query_flow_to_store_data_in_database(client: SocrataAPI, query_limit: int, query_offset: int, df: pd.DataFrame) -> list:
    # If there are no data in the database table perform a first query with data from the beginning (ORDER BY data_as_of ASC).
    if len(df) == 0:
        print('Empty database. Data from the beggining is needed in this case.')
        query = f"SELECT id, speed, travel_time, status, data_as_of \
                  ORDER BY data_as_of ASC, id ASC \
                  LIMIT {query_limit} \
                  OFFSET {query_offset}"
    # If there are data in the database look for last observation for each ID and perform queries with larger dates.
    else:
        print('Database is not empty. Data with larger date for each ID is needed in this case.')
        query = 'SELECT id, speed, travel_time, status, data_as_of WHERE'
        query += ' '
        counter = 0
        for link_id, most_recent_date in zip([x for x in df['link_id']], [x for x in df['most_recent_date']]):
            #print(f'Link ID: {link_id}, Most recent date: {most_recent_date}')
            counter += 1
            query += f"(id = '{link_id}' AND data_as_of > '{most_recent_date.isoformat()}')"
            if counter != len(df):
                query += ' '
                query += 'OR'
                query += ' '
            else:
                query += ' '
                query += 'ORDER BY data_as_of ASC'
                query += ' '
                query += f'LIMIT {query_limit} OFFSET {query_offset}'
    
    response = client.perform_query(query)
    print(response[0], response[-1])
    valid = len([x for x in response if x['status'] != '-101'])
    print(f'Valid observations: {valid}')
    return response