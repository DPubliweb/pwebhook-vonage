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
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timezone



app = Flask(__name__)
scheduler = BackgroundScheduler()

#scope = ['https://www.googleapis.com/auth/spreadsheets',
#         "https://www.googleapis.com/auth/drive"]
#
#
#TYPE = os.environ.get("TYPE")
#PROJECT_ID = os.environ.get("PROJECT_ID")
#PRIVATE_KEY_ID = os.environ.get("PRIVATE_KEY_ID")
#PRIVATE_KEY = os.environ.get("PRIVATE_KEY").replace("\\n", "\n")
#CLIENT_EMAIL = os.environ.get("CLIENT_EMAIL")
#CLIENT_ID = os.environ.get("CLIENT_ID")
#AUTH_URI = os.environ.get("AUTH_URI")
#TOKEN_URI = os.environ.get("TOKEN_URI")
#AUTH_PROVIDER_X509_CERT_URL = os.environ.get("AUTH_PROVIDER_X509_CERT_URL")
#CLIENT_X509_CERT_URL = os.environ.get("CLIENT_X509_CERT_URL")
#
#creds = ServiceAccountCredentials.from_json_keyfile_dict({
#    "type": TYPE,
#    "project_id": PROJECT_ID,
#    "private_key_id": PRIVATE_KEY_ID,
#    "private_key": PRIVATE_KEY,
#    "client_email": CLIENT_EMAIL,
#    "client_id": CLIENT_ID,
#    "auth_uri": AUTH_URI,
#    "token_uri": TOKEN_URI,
#    "auth_provider_x509_cert_url": AUTH_PROVIDER_X509_CERT_URL,
#    "client_x509_cert_url": CLIENT_X509_CERT_URL
#}, scope)
#
#client = gspread.authorize(creds)
#

def csv_empty():
    load_dotenv()
    access_key = os.environ.get("AWS_ACCESS_KEY")
    secret_key = os.environ.get("AWS_SECRET_KEY")
    
    # Connect to the S3 service
    s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    
    # Supprimer le contenu du fichier CSV
    s3.put_object(Bucket='data-vonage', Key='delivery-report.csv', Body='')
    print ("csv empty")

# Planifier l'exécution de la fonction toutes les semaines


start_time = datetime(2023, 5, 22, 10, 5, 0, tzinfo=timezone.utc)  # Date et heure spécifiques pour le début de la tâche
scheduler.add_job(csv_empty, 'interval', weeks=1, next_run_time=start_time)
scheduler.start()


#@app.route('/webhooks/delivery-receipt', methods=['GET', 'POST'])
#def delivery_receipt():
#    app.logger.info("Program running correctly")
#    if request.is_json:
#        print(request.get_json())
#    else:
#        data = dict(request.form) or dict(request.args)
#        #print(data)
#        msisdn = data['msisdn']
#        to = data['to']
#        network_code = data['network-code']
#        status = data['status']
#        err_code = data['err-code']
#        if network_code == '20801':
#            network = 'FRTE'
#        elif network_code == '20810':
#            network = 'SFR0'
#        elif network_code == '20815':
#            network = 'FREE'
#        elif network_code == '20820':
#            network = 'BOUY'
#        elif network_code == '20826':
#            network = 'NRJ'
#        elif network_code == '20827':
#            network = 'LYCA'
#        elif network_code == '20830':
#            network = 'SMAA'
#        elif network_code == '20838':
#            network = 'LEFR'
#        elif network_code == '20822':
#            network = 'TRAT'
#        elif network_code == '20831':
#            network = 'MUND'
#        elif network_code == '20824':
#            network = 'MOQU'
#        elif network_code == '20817':
#            network = 'LEGO'
#        elif network_code == '20834':
#            network = 'CEHI'
#        else:
#            network = 'null'
#        load_dotenv()
#        access_key = os.environ.get("AWS_ACCESS_KEY")
#        secret_key = os.environ.get("AWS_SECRET_KEY")
#        # Connect to the S3 service
#        s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
#        # Read the existing data from the S3 bucket
#        existing_data = s3.get_object(Bucket='data-vonage', Key='delivery-report.csv')['Body'].read().decode('utf-8')
#        
#        # Add the new data to the existing data
#        #new_data = [[phone, statut, date]]
#        if network != 'null':
#            new_data = [[msisdn,status,network]]
#        else:
#            new_data = [[msisdn,status]]
#
#        
#        # Write the updated data to the S3 bucket
#        csvfile = io.StringIO()
#        writer = csv.writer(csvfile)
#        for line in csv.reader(existing_data.splitlines()):
#            writer.writerow(line)
#        writer.writerows(new_data)
#        s3.put_object(Bucket='data-vonage', Key='delivery-report.csv', Body=csvfile.getvalue()) 
#
#        return "Done DR !"

@app.route('/webhooks/inbound-sms', methods=['GET', 'POST'])
def inbound_sms():
    if request.is_json:
        data = request.get_json
        msisdn = data['msisdn']
        text = data['text']
        keyword =  data['keyword']
        message_timestamp = data['message-timestamp']
        date = message_timestamp[:10]

        print(request.get_json(), 'salut2')
        load_dotenv()
        access_key = os.environ.get("AWS_ACCESS_KEY")
        secret_key = os.environ.get("AWS_SECRET_KEY")
        # Connect to the S3 service
        s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
        # Read the existing data from the S3 bucket
        existing_data = s3.get_object(Bucket='data-vonage', Key='stops-report.csv')['Body'].read().decode('utf-8')
        new_data = [[msisdn,keyword,date]]
        
        # Write the updated data to the S3 bucket
        csvfile = io.StringIO()
        writer = csv.writer(csvfile)
        for line in csv.reader(existing_data.splitlines()):
            writer.writerow(line)
        writer.writerows(new_data)
        s3.put_object(Bucket='data-vonage', Key='stops-report.csv', Body=csvfile.getvalue()) 

        return "Done SR !"
    else:
        return "none"


if __name__ == "__main__":
    app.run(host='0.0.0.0',port=8080,debug=True)
