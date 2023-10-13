import pandas as pd
import mysql.connector
from mysql.connector import Error

try:
    connection = mysql.connector.connect(
        host='localhost',
        database='workshop_02',
        user='root',
        password='Octubre03'
    )

    if connection.is_connected():
        db_info = connection.get_server_info()
        print(f'Conectado a MySQL Server versión {db_info}')

        cursor = connection.cursor()
except Error as e:
    print(f'Error al conectar a MySQL: {e}')

csv_file = 'the_grammy_awards.csv'
df = pd.read_csv(csv_file)

df.fillna({
    'year': 0,  
    'title': '',  
    'published_at': '0000-00-00',  
    'updated_at': '0000-00-00',  
    'category': '', 
    'nominee': '', 
    'artist': '', 
    'workers': 0,  
    'img': '',  
    'winner': 0  
}, inplace=True)

create_table_query = """
CREATE TABLE Grammy (
    year INT,
    title VARCHAR(255),
    published_at DATE,
    updated_at DATE,
    category VARCHAR(255),
    nominee VARCHAR(255),
    artist VARCHAR(255),
    workers INT,
    img VARCHAR(255),
    winner BOOLEAN
)
"""

try:
    cursor.execute(create_table_query)
    connection.commit()
    print('Tabla creada exitosamente')
except Error as e:
    print(f'Error al crear la tabla: {e}')

for index, row in df.iterrows():
    insert_query = """
    INSERT INTO Grammy (year, title, published_at, updated_at, category, nominee, artist, workers, img, winner)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    values = tuple(row)
    try:
        cursor.execute(insert_query, values)
        connection.commit()
    except Error as e:
        print(f'Error al insertar datos: {e}')
cursor.close()
connection.close()
print("Datos insertados con éxito.")