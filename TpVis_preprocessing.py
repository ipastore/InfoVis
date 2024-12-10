import os
import json
import pandas as pd

################### partes JSON a CSV ###################
#region

# # Ruta a los archivos JSON
# files = [
#     "Streaming_History_Audio_2015-2017_0.json",
#     "Streaming_History_Audio_2017-2018_1.json",
#     "Streaming_History_Audio_2018-2019_2.json",
#     "Streaming_History_Audio_2019-2020_3.json",
#     "Streaming_History_Audio_2020-2022_4.json",
#     "Streaming_History_Audio_2022-2023_5.json",
#     "Streaming_History_Audio_2023-2024_6.json",
#     "Streaming_History_Audio_2024_7.json",

# ]

# # Lista para almacenar los datos
# data = []

# # Leer y consolidar archivos JSON
# def load_and_clean_data(file):
#     with open(file, 'r', encoding='utf-8') as f:
#         entries = json.load(f)
#         for entry in entries:
#             data.append({
#                 "timestamp": entry.get("ts"),
#                 "country": entry.get("conn_country"),
#                 "track_name": entry.get("master_metadata_track_name"),
#                 "artist_name": entry.get("master_metadata_album_artist_name"),
#                 "album_name": entry.get("master_metadata_album_album_name"),
#                 "ms_played": entry.get("ms_played"),
#             })

# for file in files:

#     filePath = os.path.join('data', file)
#     load_and_clean_data(filePath)

# # Crear un DataFrame
# df = pd.DataFrame(data)

# # Limpiar datos
# # Filtrar registros con tiempo reproducido 0
# cleaned_df = df[df['ms_played'] > 0]

# cleaned_df = cleaned_df.copy()  # Ensure we are working with a copy

# # Convertir timestamp a datetime
# cleaned_df['timestamp'] = pd.to_datetime(cleaned_df['timestamp'], errors='coerce')

# # Proceed if the conversion was successful
# if pd.api.types.is_datetime64_any_dtype(cleaned_df['timestamp']):
#     cleaned_df['year'] = cleaned_df['timestamp'].dt.year
#     cleaned_df['month'] = cleaned_df['timestamp'].dt.month
#     cleaned_df['day'] = cleaned_df['timestamp'].dt.day
#     cleaned_df['hour'] = cleaned_df['timestamp'].dt.hour
#     cleaned_df['weekday'] = cleaned_df['timestamp'].dt.day_name()
# else:
#     print("Timestamp conversion failed.")   

# # Guardar el DataFrame limpio
# cleaned_df.to_csv('.data/spotify_cleaned_data.csv', index=False)

# print(cleaned_df.head())
#endregion

################### END partes JSON a CSV ###################

################### top arstist_by_year and songs ###################
#region

# Cargar el dataset limpio
df = pd.read_csv('./data/spotify_cleaned_data.csv')

# Top 10 artistas por año
top_artists_by_year = (
    df.groupby(['year', 'artist_name'])
    .agg(total_ms_played=('ms_played', 'sum'))
    .reset_index()
)

# Obtener el top 10 artistas por año
top_artists_by_year = (
    top_artists_by_year.sort_values(['year', 'total_ms_played'], ascending=[True, False])
    .groupby('year')
    .head(10)
)

# Agregar canciones al top de artistas
top_songs = df[df['artist_name'].isin(top_artists_by_year['artist_name'])]

# formatted timestamp 2015-11-18 01:56:33+00:00
# Convert it to  2015-11-18 01:56:33
top_songs['timestamp'] = pd.to_datetime(top_songs['timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')

# Resumir por artista y canción
top_songs = (
    top_songs.groupby(['year', 'artist_name', 'track_name', 'timestamp'])
    .agg(total_ms_played=('ms_played', 'sum'))
    .reset_index()
)

# Guardar los resultados
top_artists_by_year.to_csv('./data/top_artists_by_year.csv', index=False)
top_songs.to_csv('./data/top_songs_by_artist.csv', index=False)

# Guardar a JSON
top_artists_by_year.to_json('./data/top_artists_by_year.json', orient='records')
top_songs.to_json('./data/top_songs_by_artist.json', orient='records')

print("Archivos 'top_artists_by_year.csv' y 'top_songs_by_artist.csv' generados.")
print("Archivos 'top_artists_by_year.json' y 'top_songs_by_artist.json' generados.")
#endregion
################### END top arstist_by_year and songs ###################
