from flask import Flask, flash, request, redirect, render_template, send_file, url_for, make_response, after_this_request
import os
import pandas as pd
import boto3
import csv
import io
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timezone



app = Flask(__name__)
scheduler = BackgroundScheduler()

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


start_time = datetime(2023, 5, 25, 10, 5, 0, tzinfo=timezone.utc)  # Date et heure spécifiques pour le début de la tâche
scheduler.add_job(csv_empty, 'interval', weeks=1, next_run_time=start_time)
scheduler.start()

ROW_LIMIT = 50000

delivery_data = []  


@app.route('/webhooks/delivery-receipt', methods=['GET', 'POST'])
def delivery_receipt():
    app.logger.info("Program running correctly")
    if request.is_json:
        print(request.get_json())
    else:
        data = dict(request.form) or dict(request.args)
        #print(data)
        msisdn = data['msisdn']
        to = data['to']
        network_code = data['network-code']
        status = data['status']
        err_code = data['err-code']
        if network_code == '20801':
            network = 'FRTE'
        elif network_code == '20810':
            network = 'SFR0'
        elif network_code == '20815':
            network = 'FREE'
        elif network_code == '20820':
            network = 'BOUY'
        elif network_code == '20826':
            network = 'NRJ'
        elif network_code == '20827':
            network = 'LYCA'
        elif network_code == '20830':
            network = 'SMAA'
        elif network_code == '20838':
            network = 'LEFR'
        elif network_code == '20822':
            network = 'TRAT'
        elif network_code == '20831':
            network = 'MUND'
        elif network_code == '20824':
            network = 'MOQU'
        elif network_code == '20817':
            network = 'LEGO'
        elif network_code == '20834':
            network = 'CEHI'
        else:
            network = 'null'
        load_dotenv()

        if network != 'null':
            new_data = [[msisdn, status, network]]
        else:
            new_data = [[msisdn, status]]

        delivery_data.append(new_data)  # Ajouter les nouvelles données à la liste

        if len(delivery_data) >= ROW_LIMIT:
            write_delivery_data_to_s3()


        return "Done DR !"
    
def write_delivery_data_to_s3():
    load_dotenv()
    access_key = os.environ.get("AWS_ACCESS_KEY")
    secret_key = os.environ.get("AWS_SECRET_KEY")
    # Connect to the S3 service
    s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    # Read the existing data from the S3 bucket
    existing_data = s3.get_object(Bucket='data-vonage', Key='delivery-report.csv')['Body'].read().decode('utf-8')

    csvfile = io.StringIO()
    writer = csv.writer(csvfile)
    for line in csv.reader(existing_data.splitlines()):
        writer.writerow(line)
    for data in delivery_data:
        writer.writerows(data)
    s3.put_object(Bucket='data-vonage', Key='delivery-report.csv', Body=csvfile.getvalue())

    delivery_data.clear()  # Vider la liste après l'écriture sur S3

#@app.route('/webhooks/inbound-sms', methods=['GET', 'POST'])
#def inbound_sms():
#    if request.is_json:
#        data = request.get_json()
#        msisdn = data['msisdn']
#        text = data['text']
#        keyword =  data['keyword']
#        message_timestamp = data['message-timestamp']
#        date = message_timestamp[:10]
#
#        print(request.get_json(), 'salut1')
#        load_dotenv()
#        access_key = os.environ.get("AWS_ACCESS_KEY")
#        secret_key = os.environ.get("AWS_SECRET_KEY")
#        # Connect to the S3 service
#        s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
#        # Read the existing data from the S3 bucket
#        existing_data = s3.get_object(Bucket='data-vonage', Key='stops-report.csv')['Body'].read().decode('utf-8')
#        new_data = [[msisdn,keyword,date]]
#        
#        # Write the updated data to the S3 bucket
#        csvfile = io.StringIO()
#        writer = csv.writer(csvfile)
#        for line in csv.reader(existing_data.splitlines()):
#            writer.writerow(line)
#        writer.writerows(new_data)
#        s3.put_object(Bucket='data-vonage', Key='stops-report.csv', Body=csvfile.getvalue()) 
#
#        return "Done SR !"
#    else:
#        if request.form:
#            print(request.form, 'salut2')
#            msisdn = request.form.get('msisdn')
#            text = request.form.get('text')
#            keyword = request.form.get('keyword')
#            message_timestamp = request.form.get('message-timestamp')
#            load_dotenv()
#            access_key = os.environ.get("AWS_ACCESS_KEY")
#            secret_key = os.environ.get("AWS_SECRET_KEY")
#            # Connect to the S3 service
#            s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
#            # Read the existing data from the S3 bucket
#            existing_data = s3.get_object(Bucket='data-vonage', Key='stops-report.csv')['Body'].read().decode('utf-8')
#            new_data = [[msisdn,keyword,date]]
#            
#            # Write the updated data to the S3 bucket
#            csvfile = io.StringIO()
#            writer = csv.writer(csvfile)
#            for line in csv.reader(existing_data.splitlines()):
#                writer.writerow(line)
#            writer.writerows(new_data)
#            s3.put_object(Bucket='data-vonage', Key='stops-report.csv', Body=csvfile.getvalue()) 
#        else:
#        # Gérer le cas où la requête n'est ni au format JSON ni au format de formulaire
#            return "Requête invalide"
#

if __name__ == "__main__":
    app.run(host='0.0.0.0',port=8080,debug=True)
