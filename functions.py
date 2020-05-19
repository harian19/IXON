import os
import requests
import json
from google.cloud import storage
import pandas as pd
from io import StringIO
from datetime import datetime, date, timedelta
import base64

def get_storage_blob():
	try:
		storage_client = storage.Client()
		bucket_name = "ixon-data"
		destination_blob_name = "historical-daily-raw.csv"
		bucket = storage_client.bucket(bucket_name)
		blob = bucket.blob(destination_blob_name)
		try:
			blob.reload()
		except:
			return blob
		return blob
	except:
		print('Get blob failed')

def get_latest_csv_from_cloud():
	try:
		try:
			blob = get_storage_blob()
			csv_data_text = blob.download_as_string()
		except:
			return None
		return csv_data_text.decode('utf-8')
	except:
		print('Get csv failed')
	
def get_latest_timestamp_from_csv_data_text(csv_data_text):
	try:	
		if csv_data_text is None:
			return '2019-07-01 00:00:00'
		csv_data_bytes = StringIO(csv_data_text)
		df = pd.read_csv(csv_data_bytes, low_memory=False)
		latest_timestamp = df.time.max(0)
		return str(latest_timestamp)
	except:
		print('Get latest timestamp failed')

def get_base64_encoded_auth_string():
	try:
		user_id = os.environ['USER_ID']
		password = os.environ['PASSWORD']
		basic_auth_string = user_id + '::' + password
		encoded_basic_auth_string = base64.b64encode(basic_auth_string.encode('ascii'))
		return encoded_basic_auth_string.decode('utf-8')
	except:
		print('Get auth string failed')

def get_discovery_urls():
	try:
		headers = {
	    	'IXapi-Version': '1',
	    	'IXapi-Application': os.environ['API_KEY'],
		}
		discovery_urls_response = requests.get('https://api.ixon.net:443/', headers=headers)
		return discovery_urls_response.json()
	except:
		print('Get discovery urls failed')

def get_href_from_rel(rel):
	try:
		discovery_urls = get_discovery_urls()
		for url in discovery_urls['links']:
			if url['rel']==rel:
				return(url['href'])
		return None
	except:
		print('Get href failed')


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
	except:
		print('Get bearer token failed')

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
	except:
		print('Get lsi bearer token failed')

def get_lsi_discovery_urls():
	try:
		headers = {
		    'Accept': 'application/json',
		    'Authorization': 'Bearer ' + get_general_bearer_token(),
		}

		lsi_discovery_urls_response = requests.get('https://api.lsi.ams.dkn.ayayot.com:443/', headers=headers)
		return lsi_discovery_urls_response.json()
	except:
		print('Get lsi discovery urls failed')

def get_lsi_href_from_rel(rel):
	try:
		discovery_urls = get_lsi_discovery_urls()
		for url in discovery_urls['links']:
			if url['rel']==rel:
				return(url['href'])
		return None
	except:
		print('Get lsi href failed')

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
		return tags_response.json()
	except:
		print('Get tags data failed')

def get_lsi_body_json():
	try:
		body = {}
		device_id = os.environ['DEVICE_ID']
		body[device_id] = {}
		tags_data = get_tags_data()

		for tag in tags_data['data']:
		    body[device_id][str(tag['tagId'])] = {}
		    body[device_id][str(tag['tagId'])]['raw'] = []
		    body[device_id][str(tag['tagId'])]['raw'].append({})
		    body[device_id][str(tag['tagId'])]['raw'][0]['ref'] = tag['name']
		body_json = json.dumps(body)	
		return body_json
	except:
		print('Get lsi body failed')

def get_lsi_data_from_timestamp(latest_timestamp):
	try:
		url = get_lsi_href_from_rel('DataExportMultiple')
		lsi_bearer_token = get_lsi_bearer_token()

		querystring = {"timezone":"Europe/London","from":latest_timestamp}

		payload = get_lsi_body_json()
		headers = {
		    'Accept': "text/csv",
		    'Authorization': "Bearer " + lsi_bearer_token,
		    'Content-Type': "application/json"
		    }

		response = requests.request("POST", url, data=payload, headers=headers, params=querystring)
		return response.text
	except:
		print('Get lsi data failed')

def get_new_csv_for_upload():
	try:
		csv_old_text = get_latest_csv_from_cloud()
		
		latest_timestamp = get_latest_timestamp_from_csv_data_text(csv_old_text)
		csv_text_to_append = get_lsi_data_from_timestamp(latest_timestamp)

		if csv_old_text:
			csv_old_bytes = StringIO(csv_old_text)
			df_old = pd.read_csv(csv_old_bytes, low_memory=False)

			csv_bytes_to_append = StringIO(csv_text_to_append)
			df_to_append = pd.read_csv(csv_bytes_to_append, low_memory=False)
			df_to_append = df_to_append.loc[df_to_append['time'] > latest_timestamp]

			df_new = df_old.append(df_to_append, ignore_index = True)
			print(str(df_to_append.time.count()) +' rows appended to ' + str(df_old.time.count()) + ' rows and total rows= ' + str(df_new.time.count()))
			return df_new.to_csv(index=False)
		else:
			print('Full reload')
			return csv_text_to_append
	except:
		print('Get new csv failed')

def upload_to_blob(request):
	blob=get_storage_blob()
	csv_new = get_new_csv_for_upload()
	try:
		blob.upload_from_string(csv_new, content_type='text/csv')
	except:
		print('Upload failed')
	blob.make_public()


#def __init__():
#	pass







