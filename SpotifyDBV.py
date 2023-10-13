import pandas as pd
import json
import logging

def read_csv():
    SpotifyDBV = pd.read_csv("spotify_dataset.csv")
    logging.info("Extracción finalizada")
    return SpotifyDBV

def transform_dataframe(df):
    df['track_name_group'] = df['track_name'].str.lower()
    popular_songs_df = df.groupby(['track_name_group', 'artists'])['popularity'].idxmax()
    most_popular_songs = df.loc[popular_songs_df].drop(columns=['track_name_group'])
    most_popular_songs['duration'] = (most_popular_songs['duration_ms'] / 1000)
    most_popular_songs['artists'] = most_popular_songs['artists'].str.split(';').str.get(0)
    selected_columns = ['album_name', 'track_name', 'popularity', 'duration_ms', 'danceability', 'speechiness', 'acousticness',
       'instrumentalness', 'valence', 'tempo', 'track_genre']
    most_popular_songs = most_popular_songs[selected_columns]
    most_popular_songs = most_popular_songs.rename(columns={'tempo': 'tempo', 'danceability': 'danceability', 'acousticness': 'acousticness', 'speechiness': 'speechiness'})
    most_popular_songs['danceability'] = most_popular_songs['danceability']
    most_popular_songs['acousticness'] = most_popular_songs['acousticness']
    most_popular_songs['speechiness'] = most_popular_songs['speechiness']

    logging.info("Transformaciones finalizadas")
    return most_popular_songs

def main():
    SpotifyDBV = read_csv()
    transformed_df = transform_dataframe(SpotifyDBV)
    transformed_json = transformed_df.to_json(orient='records')
    return transformed_json

if __name__ == "__main__":
    result = main()
    print(result)






