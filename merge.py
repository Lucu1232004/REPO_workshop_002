import pandas as pd
import json
import mysql.connector

def merge(**kwargs):
    ti = kwargs["ti"]
    str_data_csv = ti.xcom_pull(task_ids='transform_csv')
    json_data_csv = json.loads(str_data_csv)
    data_csv = pd.json_normalize(data=json_data_csv)

    ti = kwargs["ti"]
    str_data_db = ti.xcom_pull(task_ids='transform_db')
    json_data_db = json.loads(str_data_db)
    data_db = pd.json_normalize(data=json_data_db)

    dataset_final = data_db.merge(data_csv, how='inner', 
                            left_on=['nominee', 'artist'], 
                            right_on=['track_name', 'artists'])
    
    return dataset_final.to_json(orient='records')

def load(**kwargs):
    ti = kwargs["ti"] 
    str_data=ti.xcom_pull(task_ids='load') 
    json_data_db = json.loads(str_data)
    transform_csv= pd.json_normalize(data=json_data_db)

    cnx = mysql.connector.connect(
        user='root',
        password='Octubre03',
        host='localhost',
        database='workshop_02'
    )

    create_table_query = """
    CREATE TABLE Merge (
        year INT,
        nominee VARCHAR(255),
        artist VARCHAR(255),
        winner BOOLEAN,
        album_name VARCHAR(255),
        track_name VARCHAR(255),
        popularity INT,
        track_genre VARCHAR(255),
        duration_seconds DECIMAL(10, 2)
    );
    """

    try:
        cursor = cnx.cursor()
        cursor.execute(create_table_query)
        cnx.commit()
        print("Table 'Merge' created successfully.")
    except mysql.connector.Error as err:
        print(f"Error creating table: {err}")
    finally:
        cursor.close()

    insert_query = """
    INSERT INTO Merge (year, nominee, artist, winner, album_name, track_name, popularity, track_genre, duration_seconds)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    try:
        cursor = cnx.cursor()
        dataset_final_json = merge()  

        dataset_final = json.loads(dataset_final_json)

        print("Data to be inserted:")
        print(dataset_final)

        for row in dataset_final:
            values = (
                row['year'], row['nominee'], row['artist'], row['winner'], row['album_name'],
                row['track_name'], row['popularity'], row['track_genre'], row['duration_seconds']
            )
            cursor.execute(insert_query, values)
        cursor.close()
        cnx.commit()
        print("Data inserted into the 'Merge' table successfully.")
    except (Exception, mysql.connector.Error) as error:
        print(f"Error inserting data: {error}")
    finally:
        cnx.close()

load()







