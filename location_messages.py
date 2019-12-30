import datetime
import os
import logging
import pyodbc
from google import protobuf
import mysql.connector
import random
import urllib3
import certifi
import chardet
import requests
import pymysql
import six
import sqlalchemy
import azure.functions as func
import requests
import urllib.parse
import json  
import time
import pymysql.cursors
import csv
import pandas as pd
from pandas.io import sql
import sys
import time
from datetime import timedelta
from sqlalchemy import create_engine
import six.moves.urllib as urllib 

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)


    sqlHost = os.environ.get('sqlHost')
    sqlUser = os.environ.get('sqlUser')
    sqlPassword = os.environ.get('sqlPassword')
    sqlDatabase = os.environ.get('sqlDatabase')


    connection = pymysql.connect(
    host=sqlHost,
    user = sqlUser,
    password = sqlPassword,
    db = sqlDatabase,
    ssl={'ssl': {'ssl-ca': '/var/www/html/BaltimoreCyberTrustRoot.crt.pem'}},
    cursorclass=pymysql.cursors.DictCursor)

    c = connection.cursor()
    c.execute("SELECT * FROM nov_location_messages")
    messages = c.fetchall()
    c.execute("SELECT * FROM users_p")
    users_db = c.fetchall()
    c.execute("SELECT * FROM location_events  WHERE timestamp IN (SELECT MAX(timestamp) FROM location_events GROUP BY user_id) AND user_id <> 'null'")
    location = c.fetchall()

    #get token
    import six.moves.urllib as urllib
    TOKEN = ""
    URL = "https://api.telegram.org/bot{}/".format(TOKEN)
    def get_url(url):
        response = requests.get(url)
        content = response.content.decode("utf8")
        return content

    def send_message(text, chat_id, reply_markup=None): #from article 
        text = urllib.parse.quote_plus(text)
        url = URL + "sendMessage?text={}&chat_id={}&parse_mode=Markdown".format(text, chat_id)
        if reply_markup:
            url += "&reply_markup={}".format(reply_markup)
        get_url(url)


    for i in users_db:
        for r in location:
            if i['trial_id'] == r['user_id']:
                logging.info("true")
                r['user_name'] = i['name']
                r['send_id'] = i['id']
                r['group'] = i['group']

    weekno = datetime.datetime.today().weekday()#indicates the week number to check for weekdays against weekends
    #m = datetime.datetime.now() + timedelta(hours=2)
    m = datetime.datetime.now()
    today_time = datetime.datetime.now() 
    today_time = today_time.strftime('%H:%M')
    today_date = datetime.datetime.now().strftime('%Y-%m-%d')
    location = [s for s in location if s['timestamp'].date() == datetime.datetime.today().date()]

    for l in location:
        l["timestamp"] = m + timedelta(hours=1) - l['timestamp']
        if l['location_id'] not in {57,58,59,66,67,68,70,75,76}:
            l['location_id'] = 'park'
        elif l['location_id'] in {57,58,59}:
            l['location_id'] = 'bos'
        elif l['location_id'] in {66,67,68,70}:
            l['location_id'] = 'wandelpad'
        else:
            l['location_id'] = 'bos'

    #check if location event occured in the last two hours 
    location = [l for l in location if l['timestamp'].seconds < 3600]
    logging.info(location)
    weekno = datetime.datetime.today().weekday()
    for user in location:
        ready_messages = []
        for message in messages:
            if (weekno == message['day_1'] or weekno == message['day_2'] or str(weekno) in message['day_1']):
                if user['location_id'] == message['type']:
                    ready_messages.append(message)
        if len(ready_messages) > 0:
            sending_message = random.choice(ready_messages)
        try:          
            send_message(sending_message['message'].format(user['user_name'], sending_message['type']), user['send_id'])
            c.execute("INSERT INTO sent_messages VALUES('{}','{}','{}','{}','{}','{}');".format(sending_message['message_id'], sending_message['message'], user['group'], user['user_id'], today_date, today_time))
            connection.commit()
            logging.info('sent message')
        except:
            logging.info("did not send message")
    try:
        del sending_message
        logging.info("deleted")
    except:
        logging.info("nothing to delete")
