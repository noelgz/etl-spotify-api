import spotipy
from spotipy.oauth2 import SpotifyOAuth
from datetime import datetime
import json
from elasticsearch import Elasticsearch, RequestError

import configparser
cfg = configparser.ConfigParser()
cfg.read('secrets.ini')


sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
                                              client_id = cfg['SPOTIFY']['SPOTIFY_CLIENT_ID'],
                                              client_secret = cfg['SPOTIFY']['SPOTIFY_CLIENT_SECRET'],
                                              redirect_uri = cfg['SPOTIFY']['SPOTIFY_REDIRECT_URI'],
                                              scope = cfg['SPOTIFY']['SPOTIFY_SCOPE']
                                              ))


def _create_conection_elastic(host='localhost', port=9200):
  
  es = Elasticsearch([
      {'host': cfg['ELASTIC_SETTING']['HOST'], 'port': cfg['ELASTIC_SETTING']['PORT']}
    ])
  
  return es


def _create_index_elastic():

  es = _create_conection_elastic()

  body = {
    'settings' : {
        'number_of_shards': 3,
        'number_of_replicas': 1
    },

    'mappings': {
      'properties': {
        'artist_name': {'type': 'text'},
        'id': {'type': 'text'},
        'popularity': {'type': 'integer'},
        'followers_number': {'type': 'text'},
        'href_url': {'type': 'text'},
        'uri': {'type': 'text'},
        'date_load': {'format': 'dd/MM/yyyy HH:mm:ss', 'type': 'date'},
      }
    }
  }

  try:
    print(". . . Creating artists-related-spotify index ...")
    es.indices.create(index='artists-related-spotify', body=body)

  except RequestError as es1:
    print('. . . artists-related-spotify index already exists')
  
  return es
  

def _extract(artist_id):
  
  return sp.artist_related_artists(artist_id)


def _transform(payload, current_date):

  data = []

  for artist in payload['artists']:
    data.append(
      {
        "artist_name": artist['name'],
        "id": artist['id'],
        "popularity": artist['popularity'],
        "followers_number": artist['followers']['total'],
        "href_url": artist['href'],
        "uri": artist['uri'],
        "date_load": (current_date.strftime("%d/%m/%Y %H:%M:%S"))
      }
    )
  json_object = json.dumps(data, indent=4)

  return json_object


def _load(data):

  es = _create_conection_elastic()
  ci = _create_index_elastic()

  print(". . . Loading json data in elasticsearch")

  try:
    for item in json.loads(data):
      es.index(index='artists-related-spotify', body=item)
  
  except:
    print("Something wrong :-( ")
    return None
  
  print("Data inserted correctly ...")


if __name__ == '__main__':

  now = datetime.now()

  # Exctract
  payload = _extract('3p7Bs02UWDt5ENoJeUGqaB')

  # Trasform
  json_data = _transform(payload, now)
  
  # Load
  _load(json_data)