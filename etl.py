import pandas as pd
import json
import mysql.connector
import re

def read_csv():
    df = pd.read_csv("spotify_dataset.csv")
    return df.to_json(orient="records")

def transform_csv(**kwargs):
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="read_csv")
    json_data = json.loads(str_data)
    data = pd.json_normalize(data=json_data)
    # TODO
    return data.to_json(orient="records")

def read_db():
    connection = mysql.connector.connect(
        host='localhost',
        database='workshop_02',
        user='root',
        password='pasword'
    )
    cursor = connection.cursor()
    cursor.execute('SELECT * FROM grammy')
    rows = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    df = pd.DataFrame(rows, columns=columns)
    connection.close()
    return df.to_json(orient='records')

def transform_db(**kwargs):
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids="read_db")
    json_data = json.loads(str_data)
    grammy = pd.json_normalize(data=json_data)

    grammy.dropna(subset=['artist'], inplace=True)

    def eliminar_coma(entrada):
        return re.sub(r',.*', '', entrada)

    def eliminar_punto(entrada):
        return re.sub(r';.*', '', entrada)

    grammy['artist'] = grammy['artist'].apply(eliminar_coma)
    grammy['artist'] = grammy['artist'].apply(eliminar_punto)

    columnas_a_eliminar = ['title', 'published_at', 'updated_at', 'img', 'workers']
    grammy = grammy.drop(columnas_a_eliminar, axis=1)

    return grammy.to_json(orient='records')

def merge(**kwargs):
    ti = kwargs["ti"]
    str_data1 = ti.xcom_pull(task_ids="transform_csv")
    json_data1 = json.loads(str_data1)
    data1 = pd.json_normalize(data=json_data1)
    str_data2 = ti.xcom_pull(task_ids="transform_db")
    json_data2 = json.loads(str_data2)
    data2 = pd.json_normalize(data=json_data2)

    merge_df = data1.merge(data2, how="inner", left_on='track_name', right_on='artist')
    return merge_df.to_json(orient="records")

def load_to_db():
    # TODO
    connection = mysql.connector.connect(
        host='localhost',
        database='workshop_02',
        user='root',
        password='Octubre03'
    )
    cursor = connection.cursor()
    connection.close()
    return "Data loaded successfully"

def store_result(result_data):
    with open('result_data.json', 'w') as outfile:
        json.dump(result_data, outfile)
    return "Data stored successfully"
