import os
import sys
import jsonlines
import time
import uuid
from datetime import datetime, timedelta

currentdir = os.path.abspath(os.path.dirname(__file__))
parentdir = os.path.abspath(os.path.join(currentdir, os.pardir))

sys.path.insert(0, parentdir + '\\' + 'utils')

try:
    from utils.db_utils import SocrataAPI
except ModuleNotFoundError:
    from db_utils import SocrataAPI

real_time_traffic_temp_path = parentdir + '\\' + 'data' + '\\' + 'temp_real_time_traffic_data'

for file in os.listdir(real_time_traffic_temp_path):
    os.remove(real_time_traffic_temp_path + '\\' + file)

while True:
    now_in_nyc = datetime.now() - timedelta(hours = 6) # Get current time in NYC
    date_threshold = (now_in_nyc - timedelta(hours = 2)).replace(microsecond = 0) # Substrate 2 hours to get data of two previous hours
    client = SocrataAPI(dataset_identifier = 'i4gi-tjb9', content_type = 'json')
    query = f"SELECT id, speed, status, travel_time, data_as_of WHERE data_as_of > '{date_threshold.isoformat()}' ORDER BY data_as_of DESC LIMIT 50000"
    response = client.perform_query(query)
    if len(response) == 0:
        print('Empty data. New query is necessary.')
    else:
        with jsonlines.open(f'{real_time_traffic_temp_path}\\{str(uuid.uuid4())}.json', 'w') as f:
            f.write_all(response)
        print('Waiting 1 minute...')
        time.sleep(55)
    time.sleep(5)