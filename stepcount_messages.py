
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
from sqlalchemy import create_engine

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
        ssl={'ssl': {'ssl-ca': '/var/www/html/BaltimoreCyberTrustRoot.crt.pem'}},
        cursorclass=pymysql.cursors.DictCursor)

    weekno = datetime.datetime.today().weekday()#indicates the week number to check for weekdays against weekends
    c = connection.cursor()
    c.execute("SELECT * FROM nov_step_messages WHERE day_1 = '{}' OR day_2 = '{}'".format(weekno, weekno))
    messages = c.fetchall()
    c.execute("SELECT * FROM users_p")
    users_db = c.fetchall()
    c.execute("SELECT * FROM stepcount WHERE user_id <> 'null'")
    steps = c.fetchall()

    for i in users_db:
        for r in steps:
            if i['trial_id'] == r['user_id']:
                r['user_name'] = i['name']
                r['group'] = i['group']
                r['send_id'] = i['id']

    for r in steps:
        if r['daily_stepcount'] < 2000:
            r['step_range'] = 'range 1'
        elif 2001 <= r['daily_stepcount'] <= 6000:
            r['step_range'] = 'range 2'
        else:
            r['step_range'] = 'range 3'

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
    weekno = datetime.datetime.today().weekday()
    #one hour ahead 
    today_time = datetime.datetime.now()
    today_time = today_time.strftime('%H:%M')
    today_date = datetime.datetime.now().strftime('%Y-%m-%d')
    for user in steps: 
        all_included = []
        exclusive = []
        for message in messages:
            if user['step_range'] == message['range'] and (message['day_1'] == weekno or message['day_2'] == weekno) and user['date'].strftime('%Y-%m-%d') == today_date:
                if message['time_two'] == today_time:
                    all_included.append(message)
                elif message['time_one'] == today_time:
                    exclusive.append(message)
        if len(all_included) > 0:
            sending_message_all = random.choice(all_included)
        if len(exclusive) > 0:
            sending_message_ex = random.choice(exclusive)
        try:
            send_message(sending_message_all['message'].format(user['user_name'], user['daily_stepcount']), user['send_id'])
            c.execute("INSERT INTO sent_messages VALUES('{}','{}','{}','{}','{}','{}');".format(sending_message_all['message_id'], sending_message_all['message'], user['group'], user['user_id'], today_date, today_time))
            connection.commit()
            logging.info(sending_message_all)
        except:
            pass
        if user['group'] != 'Group 1':
            try:
                send_message(sending_message_ex['message'].format(user['user_name'], user['daily_stepcount']), user['send_id'])
                logging.info(sending_message_ex)
                c.execute("INSERT INTO sent_messages VALUES('{}','{}','{}','{}','{}','{}');".format(sending_message_ex['message_id'], sending_message_ex['message'], user['group'], user['user_id'], today_date, today_time))
                connection.commit()
            except:
                pass
    try:
        del sending_message_all
    except:
        logging.info('nothing to delete')
    try:
        del sending_message_ex
    except:
        logging.info('nothing to delete')


