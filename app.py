from flask import Flask, flash, request, redirect, render_template, send_file, url_for, make_response, after_this_request
import json
import os
import pandas as pd
import boto3
import csv
import io
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2.credentials import Credentials
import google.auth
from dotenv import load_dotenv


app = Flask(__name__)

scope = ['https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive"]


TYPE = os.environ.get("TYPE")
PROJECT_ID = os.environ.get("PROJECT_ID")
PRIVATE_KEY_ID = os.environ.get("PRIVATE_KEY_ID")
PRIVATE_KEY = os.environ.get("PRIVATE_KEY").replace("\\n", "\n")
CLIENT_EMAIL = os.environ.get("CLIENT_EMAIL")
CLIENT_ID = os.environ.get("CLIENT_ID")
AUTH_URI = os.environ.get("AUTH_URI")
TOKEN_URI = os.environ.get("TOKEN_URI")
AUTH_PROVIDER_X509_CERT_URL = os.environ.get("AUTH_PROVIDER_X509_CERT_URL")
CLIENT_X509_CERT_URL = os.environ.get("CLIENT_X509_CERT_URL")

creds = ServiceAccountCredentials.from_json_keyfile_dict({
    "type": TYPE,
    "project_id": PROJECT_ID,
    "private_key_id": PRIVATE_KEY_ID,
    "private_key": PRIVATE_KEY,
    "client_email": CLIENT_EMAIL,
    "client_id": CLIENT_ID,
    "auth_uri": AUTH_URI,
    "token_uri": TOKEN_URI,
    "auth_provider_x509_cert_url": AUTH_PROVIDER_X509_CERT_URL,
    "client_x509_cert_url": CLIENT_X509_CERT_URL
}, scope)

client = gspread.authorize(creds)


@app.route('/webhooks/delivery-receipt', methods=['GET', 'POST'])
def delivery_receipt():
    if request.is_json:
        print(request.get_json())
    else:
        data = dict(request.form) or dict(request.args)
        print(data)
        #msisdn = data['msisdn']
        #to = data['to']
        #network_code = data['network-code']
        #message_id = data['messageId']
        #price = data['price']
        #status = data['status']
        #scts = data['scts']
        #err_code = data['err-code']
        #api_key = data['api-key']
        #message_timestamp = data['message-timestamp']
        #row = [msisdn, to, network_code, message_id, price, status, scts, err_code, api_key, message_timestamp]
        #print(data)
##
    return ('DLR', 200)

@app.route('/webhooks/inbound-sms', methods=['GET', 'POST'])
def inbound_sms():
    if request.is_json:
        print('json')
        print(request.get_json())
    else:
        data = dict(request.form) or dict(request.args)
        print('pas json', data)
        ##msisdn = data['msisdn']
        to = data['to']
        ##message_id = data['messageId']
        ##mt_message_id = data['mt-message-id']
        text = data['text']
        ##message_type = data['type']
        ##keyword =  data['keyword']
        ##api_key = data['api-key']
        message_timestamp = data['message-timestamp']
        row = [to, text, message_timestamp]
        keys = ["1","2","3"]
        if text in keys :
           sheet = client.open("Campagne Réno Réponses").sheet1
           sheet.append_row(row)
        else: 
            print(row)
        print(data)
    return ('Inbound', 200)



if __name__ == "__main__":
    app.run(host='0.0.0.0',port=8080,debug=False)
