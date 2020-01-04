import datetime
import logging
import os
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
#import azure.functions as func
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
from sqlalchemy import create_engine
import six.moves.urllib as urllib 

import azure.functions as func


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
            ssl={""},
            cursorclass=pymysql.cursors.DictCursor)

    c = connection.cursor()
    weekno = datetime.datetime.today().weekday()#indicates the week number to check for weekdays against weekends
    c.execute("SELECT * FROM nov_time_messages WHERE day_1 = '{}' OR day_2 = '{}'".format(weekno, weekno))
    messages = c.fetchall()
    c.execute("SELECT * FROM users_p")
    users_db = c.fetchall()
                
    #get token
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

    def check_time(hours, minutes, interval):
        time_now = datetime.datetime.now().time()
        time_start = datetime.datetime(100, 1, 1, hours, minutes)
        time_end = time_start + datetime.timedelta(minutes = interval)
        return(time_start.time() < time_now < time_end.time())
    ##this is different stuff 
    def get_updates(offset=None): #found in article
        url = URL + "getUpdates"
        js = get_json_from_url(url)
        return js

    def get_json_from_url(url): ##found in article 
        content = get_url(url)
        js = json.loads(content)
        return js

        
    weekno = datetime.datetime.today().weekday()#indicates the week number to check for weekdays against weekends
    today_time = datetime.datetime.now()
    today_time = today_time.strftime('%H:%M')
    today_date = datetime.datetime.now().strftime('%Y-%m-%d')
    weekno = datetime.datetime.today().weekday()
    for user in users_db:
        all_included = []
        exclusive = []
        group_three = []
        for message in messages:
            if (message['day_1'] == weekno or message['day_2'] == weekno):
                    if today_time == message['time_1']:
                        all_included.append(message)
                    if today_time == message['time_2_2']:
                        exclusive.append(message)
                    if (today_time == message['time_3_1'] or today_time == message['time_3_2']):
                        group_three.append(message)
        if len(all_included) > 0:
            sending_message_all = random.choice(all_included)
        if len(exclusive) > 0:
            sending_message_two = random.choice(exclusive)
        if len(group_three) > 0:
            sending_message_three = random.choice(group_three)
        if (user['group'] == 'Group 1'):
            try:
                send_message(sending_message_all['message'].format(user['name']), user['id'])
                logging.info(sending_message_all)
                c.execute("INSERT INTO sent_messages VALUES('{}','{}','{}','{}','{}','{}');".format(sending_message_all['message_id'], sending_message_all['message'], user['group'], user['trial_id'], today_date, today_time))
                connection.commit()
            except:
                logging.info("did not send first")
        if user['group'] == 'Group 2':
            try: 
                send_message(sending_message_two['message'].format(user['name']), user['id'])
                logging.info(sending_message_two)
                c.execute("INSERT INTO sent_messages VALUES('{}','{}','{}','{}','{}','{}');".format(sending_message_two['message_id'], sending_message_two['message'], user['group'], user['trial_id'], today_date, today_time))
                connection.commit()
            except:
                logging.info("did not send second")
        if user['group'] == "Group 3":
            try: 
                send_message(sending_message_three['message'].format(user['name']), user['id'])
                logging.info(sending_message_three)
                c.execute("INSERT INTO sent_messages VALUES('{}','{}','{}','{}','{}','{}');".format(sending_message_three['message_id'], sending_message_three['message'], user['group'], user['trial_id'], today_date, today_time))
                connection.commit()
            except:
                logging.info("did not send third")

    try:
        del sending_message_all
        logging.info("deleted_message_to_first")
    except:
        logging.info('nothing to delete for first group')
    try:
        del sending_message_two
        logging.info("deleted_message_to_two")
    except:
        logging.info('nothing to delete for second group')
    try:
        del sending_message_three
        logging.info("deleted_message_to_three")
    except:
        logging.info('nothing to delete for third group')

