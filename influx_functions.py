import os
import requests
import json
import pandas as pd
from io import StringIO
from datetime import datetime, date, timedelta
import base64
from influxdb import DataFrameClient

def get_influx_client():
  try:
    HOST = os.environ['INFLUX_HOST_URL']
    USERNAME = os.environ['INFLUX_USERNAME']
    PASSWORD = os.environ['INFLUX_PASSWORD']
    DB = 'test_total_4'
    PORT=8086
    client = DataFrameClient(HOST, PORT, USERNAME, PASSWORD, DB)
    return client
  except Exception as e:
    print('Get influx client failed')
    print(e)

  
def get_latest_timestamps_from_influx(client, tags):
  MEASUREMENT = os.environ['MEASUREMENT']
  latest_timestamps = []
  for tag in tags:
    try:
      last_data = client.query('SELECT LAST("' + tag + '") FROM ' + MEASUREMENT)
      latest_timestamp_index = last_data[MEASUREMENT].index
      latest_timestamp = latest_timestamp_index.strftime('%Y-%m-%d %H:%M:%S.%f')[0]
      latest_timestamps.append(latest_timestamp)
    except:
      print('Could not fetch latest timestamp for tag ' + tag + '. Using default.')
      latest_timestamp = '2015-01-01 00:00:00'
      latest_timestamps.append(latest_timestamp)
  return latest_timestamps


def get_base64_encoded_auth_string():
  try:
    # user_id = os.environ['USER_ID']
    # password = os.environ['PASSWORD']
    # basic_auth_string = user_id + '::' + password
    encoded_basic_auth_string = os.environ['AUTH_STRING']
    return encoded_basic_auth_string
    # encoded_basic_auth_string = base64.b64encode(basic_auth_string.encode('ascii'))
    # return encoded_basic_auth_string.decode('utf-8')
  except Exception as e:
    print('Get auth string failed')
    print(e)


def get_discovery_urls():
  try:
    headers = {
        'IXapi-Version': '1',
        'IXapi-Application': os.environ['API_KEY'],
    }
    discovery_urls_response = requests.get('https://api.ixon.net:443/', headers=headers)
    return discovery_urls_response.json()
  except Exception as e:
    print('Get discovery urls failed')
    print(e)


def get_href_from_rel(rel):
  try:
    discovery_urls = get_discovery_urls()
    for url in discovery_urls['links']:
      if url['rel']==rel:
        return(url['href'])
    return None
  except Exception as e:
    print('Get href failed')
    print(e)


def get_general_bearer_token():
  try:
    encoded_auth_string = get_base64_encoded_auth_string()
    url = get_href_from_rel('AccessTokenList')
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'IXapi-Version': '1',
        'IXapi-Application': os.environ['API_KEY'],
        'Authorization': 'Basic '+ encoded_auth_string,
    }
    params = (
        ('fields', 'expiresIn,secretId'),
    )
    data = '{ "expiresIn": 3600}'
    bearer_token_api_response = requests.post(url, headers=headers, params=params, data=data)
    bearer_token = bearer_token_api_response.json()['data']['secretId']
    return bearer_token
  except Exception as e:
    print('Get bearer token failed')
    print(e)


def get_lsi_bearer_token():
  try:
    url = get_href_from_rel('AuthorizationTokenList')
    headers = {
        'IXapi-Version': '1',
        'IXapi-Application': os.environ['API_KEY'],
        'IXapi-Company': os.environ['COMPANY_ID'],
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + get_general_bearer_token(),
    }
    data = '{"expiresIn":3600, "agents":"' + os.environ['AGENT_ID'] + '"}'
    lsi_bearer_token_response = requests.post(url, headers=headers, data=data)
    lsi_bearer_token = lsi_bearer_token_response.json()['data']['token']
    return lsi_bearer_token
  except Exception as e:
    print('Get lsi bearer token failed')
    print(e)


def get_lsi_discovery_urls():
  try:
    headers = {
        'Accept': 'application/json',
        'Authorization': 'Bearer ' + get_general_bearer_token(),
    }
    lsi_discovery_urls_response = requests.get('https://api.lsi.ams.dkn.ayayot.com:443/', headers=headers)
    return lsi_discovery_urls_response.json()
  except Exception as e:
    print('Get lsi discovery urls failed')
    print(e)


def get_lsi_href_from_rel(rel):
  try:
    discovery_urls = get_lsi_discovery_urls()
    for url in discovery_urls['links']:
      if url['rel']==rel:
        return(url['href'])
    return None
  except Exception as e:
    print('Get lsi href failed')
    print(e)


def get_tags_data():
  try:
    bearer_token = get_general_bearer_token()
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'IXapi-Version': '1',
        'IXapi-Application': os.environ['API_KEY'],
        'IXapi-Company': os.environ['COMPANY_ID'],
        'Authorization': 'Bearer '+ bearer_token,
    }
    url = get_href_from_rel('AgentDeviceDataTagList')
    url = url.replace('{agentId}', os.environ['AGENT_ID'])
    url = url.replace('{deviceId}', os.environ['DEVICE_ID'])
    tags_response = requests.get(url, headers=headers)
    tags_response = tags_response.json()
    return tags_response['data']
  except Exception as e:
    print('Get tags data failed')
    print(e)


def get_lsi_body_json(tag):
  try:
    body = {}
    device_id = os.environ['DEVICE_ID']
    body[device_id] = {}
    body[device_id][str(tag['tagId'])] = {}
    body[device_id][str(tag['tagId'])]['raw'] = []
    body[device_id][str(tag['tagId'])]['raw'].append({})
    body[device_id][str(tag['tagId'])]['raw'][0]['ref'] = tag['name']
    body_json = json.dumps(body)  
    return body_json
  except Exception as e:
    print('Get lsi body failed for tag ' + tag['name'])
    print(e)


def get_lsi_data_from_timestamp(lsi_bearer_token, url, tag_object, latest_timestamp):
    try:
      querystring = {"timezone":"Europe/London","from":latest_timestamp }
      payload = get_lsi_body_json(tag_object)
      headers = {
          'Accept': "text/csv",
          'Authorization': "Bearer " + lsi_bearer_token,
          'Content-Type': "application/json"
          }
      response = requests.request("POST", url, data=payload, headers=headers, params=querystring)
      return response.text
    except Exception as e:
      print('Get lsi data failed for tag ' + tag_object['name'])
      print(e)


def get_dfs_to_append(client):
  tag_objects = get_tags_data()
  tags = []
  for tag_object in tag_objects:
    tags.append(tag_object['name'])
  latest_timestamps = get_latest_timestamps_from_influx(client, tags)
  url = get_lsi_href_from_rel('DataExportMultiple')
  lsi_bearer_token = get_lsi_bearer_token()
  df_list = []
  for i in range(len(tag_objects)):
    try:
      csv_text_to_append = get_lsi_data_from_timestamp(lsi_bearer_token, url, tag_objects[i], latest_timestamps[i])
      csv_bytes_to_append = StringIO(csv_text_to_append)
      df_to_append = pd.read_csv(csv_bytes_to_append, low_memory=False)
      df_to_append['time'] = pd.to_datetime(df_to_append['time'], format="%Y-%m-%d %H:%M:%S.%f")
      df_to_append_indexed = df_to_append.set_index(pd.DatetimeIndex(df_to_append.time.sort_index()))
      df_to_append_time_dropped = df_to_append_indexed.drop(columns=['time'])
      df = df_to_append_time_dropped
      print(str(df.shape[0]) +' rows to be pushed for tag ' + tags[i])
      df_list.append(df)
    except Exception as e:
      print('Get df to append failed for tag ' + tags[i])
      print(e)
  return df_list


def upload_df_to_influxdb(client, df):
  try:
    protocol = 'line'
    DB = 'test_total_4'
    MEASUREMENT = os.environ['MEASUREMENT']
    client.create_database(DB)
    client.write_points(df, MEASUREMENT, protocol=protocol, batch_size = 5000)
  except Exception as e:
    print('Write to influxdb failed')
    print(e)


def run_pipeline(request):
  try:
    client = get_influx_client()
    dfs_to_append = get_dfs_to_append(client)
    for df in dfs_to_append:
      upload_df_to_influxdb(client, df)
  except Exception as e:
    print('Pipeline failed')
    print(e)

def __init__():
  pass




