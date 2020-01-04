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
import requests
import urllib.parse
import json  
import time
import pymysql.cursors
import csv
from datetime import timedelta
import pandas as pd
from pandas.io import sql
import sys
from sqlalchemy import create_engine
import re
import itertools
from itertools import chain
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

    TOKEN = ""
    URL = "https://api.telegram.org/bot{}/".format(TOKEN)

    connection = pymysql.connect(
    host=sqlHost,
    user = sqlUser,
    password = sqlPassword,
    db = sqlDatabase,
    ssl={""},
    cursorclass=pymysql.cursors.DictCursor)
    c = connection.cursor()

    def update_tables(sql_statement):
        c = connection.cursor()
        c.execute(sql_statement)
        return connection.commit()

    def get_url(url):
        response = requests.get(url)
        content = response.content.decode("utf8")
        return content

    def get_last_update_id(updates):
        num_updates = len(updates["result"])
        last_update = num_updates - 1
        chat_id = updates["result"][last_update]["message"]["message_id"]
        return chat_id

    def get_json_from_url(url):
        content = get_url(url)
        js = json.loads(content)
        return js

    def get_updates(offset=112691679):
        url = URL + "getUpdates?timeout=100"
        if offset:
            url += "&offset={}".format(offset)
        js = get_json_from_url(url)
        return js
        
    def send_message(text, chat_id, reply_markup=None):
        text = urllib.parse.quote_plus(text)
        url = URL + "sendMessage?text={}&chat_id={}&parse_mode=Markdown".format(text, chat_id)
        if reply_markup:
            url += "&reply_markup={}".format(reply_markup)
        get_url(url)

    updates = get_updates()
    user_exists = []
    text_id = []
    text_start = []
    incorrect_id = []
    c.execute("SELECT id FROM users_p")
    sorted_ids = c.fetchall()
    sorted_ids = [x['id'] for x in sorted_ids]
    c.execute("SELECT * from start_done")
    temporary_id = c.fetchall()
    c.execute("SELECT * from wrong_ids")
    wrong_ids = c.fetchall()
    wrong_id = [x['wrong_id'].split(',') for x in wrong_ids]
    wrong_id = list(chain.from_iterable(wrong_id))
    wrong_format = [x['wrong_format'].split(',') for x in wrong_ids]
    wrong_format = list(chain.from_iterable(wrong_format))
    temporary_id = [x['id'] for x in temporary_id]

    for update in updates["result"]:
        if update["message"]["text"] == "/start" and update['message']['chat']["id"] not in sorted_ids and update['message']['chat']["id"] not in temporary_id:
            text_start.append(update["message"]["chat"])
            text_start = [i for n, i in enumerate(text_start) if i not in text_start[n + 1:]]
        elif re.match(r"AMS_00\d.", update["message"]["text"]) and update['message']['chat']['id'] not in sorted_ids:
            text_id.append((update["message"]["chat"], update['message']['text']))
        elif update["message"]["text"] != "/start" and re.match(r"AMS_00\d.", update["message"]["text"]) == None:
            incorrect_id.append((update['message']['chat'], update['message']['text']))
    for user in text_start:
            send_message("Hi, please send me your participant ID.", user['id'])
            update_tables("INSERT INTO start_done (id) VALUES ({})".format(user['id']))
    for user in text_id:
        try:
            c.execute("SELECT * FROM users_p WHERE trial_id = '{}'".format(user[1]))
            user_exists = c.fetchall()
        except:
            logging.info("nope")
        if len(user_exists) > 0:
            send_message("Thank you for sharing your participant number.", user[0]['id'])
            update_tables("UPDATE users_p SET id = {} WHERE trial_id = '{}'".format(user[0]["id"], user[1]))
            update_tables("DELETE FROM wrong_ids WHERE telegram_id = '{}'".format(user[0]['id']))
            user_exists = []
        elif (not wrong_ids or user[1] not in wrong_id) and user[0]['id'] not in user_exists:
            send_message("This number was not correct. Please try again.", user[0]['id'])
            update_tables("INSERT IGNORE INTO wrong_ids (telegram_id) VALUES('{}')".format(user[0]['id']))
            update_tables("UPDATE wrong_ids SET wrong_id = concat(wrong_id, ',{}') WHERE telegram_id = '{}'".format(user[1], user[0]['id']))
    for user in incorrect_id:
        if (not wrong_format or user[1] not in wrong_format) and user[0]['id'] not in sorted_ids:
            send_message("That does not look like the correct format. Check the instructions again", user[0]['id'])
            update_tables("INSERT IGNORE INTO wrong_ids (telegram_id) VALUES('{}')".format(user[0]['id']))
            update_tables("UPDATE wrong_ids SET wrong_format = concat(wrong_format, ',{}') WHERE telegram_id = '{}'".format(user[1], user[0]['id']))
    
    