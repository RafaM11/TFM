import os
import sys
import pandas as pd

currentdir = os.path.abspath(os.path.dirname(__file__))
parentdir = os.path.abspath(os.path.join(currentdir, os.pardir))

sys.path.insert(0, parentdir + '\\' + 'utils')

try:
    from utils.db_utils import DBConnection, SocrataAPI
    from utils.misc_utils import get_distance_from_polyline
except ModuleNotFoundError:
    from db_utils import DBConnection, SocrataAPI
    from misc_utils import get_distance_from_polyline

def create_tb_links():
    client = SocrataAPI(dataset_identifier = 'i4gi-tjb9', content_type = 'json')
    query = "SELECT id, borough, encoded_poly_line, COUNT(*) AS count GROUP BY id, borough, encoded_poly_line LIMIT 50000"
    response = client.perform_query(query)
    links_df = pd.DataFrame(response)
    links_df = links_df.astype({'id': 'int64', 'count': 'int64'}).sort_values(by = 'id')

    non_repeated_polyline_links_df = links_df[links_df['id'].isin(links_df['id'].value_counts().index[links_df['id'].value_counts() == 1])].sort_values(by = 'id')

    repeated_polyline_links_df = links_df[~links_df['id'].isin(links_df['id'].value_counts().index[links_df['id'].value_counts() == 1])].sort_values(by = 'id')
    repeated_polyline_links_df['\ count'] = [polyline.count('\\') for polyline in repeated_polyline_links_df['encoded_poly_line']]

    gb = repeated_polyline_links_df.groupby('id')

    best_polyline_df = pd.DataFrame()
    for link_id in gb.groups.keys():
        filtered_df = repeated_polyline_links_df[(repeated_polyline_links_df['id'] == link_id)]
        best_polyline_df = best_polyline_df.append(filtered_df[filtered_df.index == filtered_df['\ count'].idxmin()])

    links_df = pd.concat([non_repeated_polyline_links_df, best_polyline_df], axis = 0).sort_values(by = 'id').reset_index(drop = True)
    links_df = links_df.drop(columns = ['count', '\ count'])

    links_df['encoded_poly_line'] = [x.replace('\\\\', '\\') for x in links_df['encoded_poly_line']]

    links_df.at[links_df['encoded_poly_line'][links_df['id'] == 184].index[0], 'encoded_poly_line'] = links_df.at[links_df['encoded_poly_line'][links_df['id'] == 184].index[0], 'encoded_poly_line'][:-1]
    links_df.at[links_df['encoded_poly_line'][links_df['id'] == 212].index[0], 'encoded_poly_line'] = links_df.at[links_df['encoded_poly_line'][links_df['id'] == 212].index[0], 'encoded_poly_line'][:-1]

    links_df['points'] = [get_distance_from_polyline(x)[0] for x in links_df['encoded_poly_line']]
    links_df['distance'] = [get_distance_from_polyline(x)[1] for x in links_df['encoded_poly_line']]

    db = DBConnection()
    conn = db.connect()
    cur = conn.cursor()
    for idx in links_df.index:
        link_id = int(links_df['id'][idx])
        borough = links_df['borough'][idx].title()
        points = links_df['points'][idx]
        distance = links_df['distance'][idx]
        encoded_poly_line = links_df['encoded_poly_line'][idx]
        insert_data = "INSERT INTO tb_links (link_id, borough, points, distance, encoded_poly_line) VALUES(%s, %s, ARRAY[%s]::coordinates[], %s, %s)"
        cur.execute(insert_data, (link_id, borough, points, distance, encoded_poly_line))
    conn.commit()
    cur.close()
    conn.close()

if __name__ == '__main__':
    create_tb_links()