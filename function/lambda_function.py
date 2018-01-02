from __future__ import (print_function)

import os
import time
import feedparser
import boto3
import requests

HOOK_URL = os.environ['BOT_URL']
TABLE_NAME = os.environ['TABLE_NAME']
DB = boto3.resource('dynamodb')

def load_new_items():
    feed = feedparser.parse('https://aws.amazon.com/new/feed/')
    keys = []
    items = {}

    epoch_time = int(time.time()) + 2592000 
    for entry in feed['entries']:
        id = entry['title_detail']['value'].lower()
        keys.append({'id': id})
        items[id] = {
            'id': id,
            'expire': epoch_time,
            'message': entry['title_detail']['value'] + ': ' +  entry['link']
        }

    response = DB.batch_get_item(
        RequestItems={TABLE_NAME: {'Keys': keys}},
        ReturnConsumedCapacity='TOTAL'
    )
    print('DynamoDB read capacity used: ', response['ConsumedCapacity'])

    if 'Responses' in response:
        for item in response['Responses'][TABLE_NAME]:
            del items[item['id']]

    dynamodb_items = []
    for key, value in items.items():
        dynamodb_items.append({'PutRequest': {'Item': value}})

    if len(items) > 0:
        response = DB.batch_write_item(
            RequestItems={
                TABLE_NAME: dynamodb_items
            },
            ReturnConsumedCapacity='TOTAL'
        )
        print('DynamoDB write capacity used: ', response['ConsumedCapacity'])
    return items


def lambda_handler(event, context):
    new_messages = load_new_items()
    content = ""
    if  len(new_messages) > 0 and len(new_messages) < 20:
        i = 0
        for k, message in new_messages.items():
            content += "# " + message['message']
            i += 1
            if len(new_messages) > i:
                content += "\n\n"
        response = requests.post(url=HOOK_URL, json={"Content": content})
    elif len(new_messages) >= 20:
        response = requests.post(url=HOOK_URL, json={"Content": "Too many new messages... Check the site: https://aws.amazon.com/new. "})

    print(content)
