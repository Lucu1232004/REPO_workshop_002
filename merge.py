import pandas as pd
import mysql.connector
import numpy as np

def create_connection():
    cnx = mysql.connector.connect(
        user='root',
        password='Octubre03',
        host='localhost',
        database='workshop_02'
    )
    return cnx


create_table_query = """
CREATE TABLE Merge (
    id INT AUTO_INCREMENT PRIMARY KEY,
    winner BOOLEAN,
    album_name VARCHAR(1000),
    track_name VARCHAR(1000),
    popularity INT,
    track_genre VARCHAR(1000),
    duration_ms DECIMAL(10, 2)
);
"""

insert_query = """
INSERT INTO Merge (winner, album_name, track_name, popularity, track_genre, duration_ms)
VALUES (1, %s, %s, %s, %s, %s)
"""

file1 = "spotify_dataset.csv"
file2 = "the_grammy_awards.csv"

df1 = pd.read_csv(file1, encoding='utf-8')
df2 = pd.read_csv(file2, encoding='utf-8')

combined_df = pd.concat([df1, df2], ignore_index=True)

combined_df = combined_df.replace({"Unknown": None, np.nan: None})

default_values = {
    'album_name': '',
    'track_name': '',
    'popularity': 0,
    'track_genre': '',
    'duration_ms': 0
}
combined_df.fillna(default_values, inplace=True)

cnx = create_connection()

try:
    cursor = cnx.cursor()

    cursor.execute(create_table_query)
    cnx.commit()
    print("Tabla 'Merge' creada exitosamente.")

    for index, row in combined_df.iterrows():
        duration_ms = None
        if not pd.isna(row['duration_ms']):
            try:
                duration_ms = float(row['duration_ms'])
            except ValueError:
                duration_ms = None

        values = (
            row['album_name'],
            row['track_name'],
            int(row['popularity']) if not pd.isna(row['popularity']) else None,
            row['track_genre'],
            duration_ms
        )

        cursor.execute(insert_query, values)

    cnx.commit()
    print("Datos insertados en la tabla 'Merge' exitosamente con 'winner' = 1.")
except (Exception, mysql.connector.Error) as error:
    print(f"Error: {error}")
finally:
    if cnx:
        cnx.close()




