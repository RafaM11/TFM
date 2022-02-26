import os
import sys
import jsonlines
import time

currentdir = os.path.abspath(os.path.dirname(__file__))
parentdir = os.path.abspath(os.path.join(currentdir, os.pardir))

sys.path.insert(0, parentdir + '\\' + 'utils')

try:
    from utils.db_utils import get_all_link_ids, DBConnection, SocrataAPI
except ModuleNotFoundError:
    from db_utils import get_all_link_ids, DBConnection, SocrataAPI

historical_traffic_temp_path = parentdir + '\\' + 'data' + '\\' + 'temp_historical_traffic_data'

client = SocrataAPI(dataset_identifier = 'i4gi-tjb9', content_type = 'json')

link_id_list = get_all_link_ids()
for link_id in link_id_list:
    print(f'####################### LINK ID: {link_id} #######################')
    db = DBConnection()
    query = f'SELECT MAX(measurement_date) AS most_recent_date FROM tb_historical_traffic WHERE link_id = {link_id}'
    query_result = db.perform_query(query = query)
    if query_result['most_recent_date'].item() is not None:
        most_recent_date = query_result['most_recent_date'].item().isoformat()
        query = f"SELECT id, speed, travel_time, status, data_as_of \
                  WHERE data_as_of > '{most_recent_date}' AND id = '{link_id}' AND status = '0' \
                  LIMIT 50000"
        response = client.perform_query(query)
        if len(response) == 0:
            print(f'All data from ID {link_id} is already stored.')
        else:
            with jsonlines.open(f'{historical_traffic_temp_path}\\{link_id}.json', 'w') as f:
                f.write_all(response)
            time.sleep(10)