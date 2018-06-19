import os
import time
import feedparser
import boto3
import requests
from io import BytesIO

HOOK_URL = os.environ['BOT_URL']
TABLE_NAME = os.environ['TABLE_NAME']
DB = boto3.resource('dynamodb')
FEEDS = [
    'https://aws.amazon.com/new/feed/',
    'https://aws.amazon.com/security/security-bulletins/feed/'
]

def check_items(keys, items):
    if len(keys) > 0:
        response = DB.batch_get_item(
            RequestItems={TABLE_NAME: {'Keys': keys}},
            ReturnConsumedCapacity='TOTAL'
        )
        keys = []
        print('DynamoDB read capacity used: ', response['ConsumedCapacity'])

        if 'Responses' in response:
            for item in response['Responses'][TABLE_NAME]:
                del items[item['id']]

    return items

def commit_items(dynamodb_items):
    print("DynamoDB Items:", dynamodb_items)
    if len(dynamodb_items) != 0:
        response = DB.batch_write_item(
            RequestItems={
                TABLE_NAME: dynamodb_items
            },
            ReturnConsumedCapacity='TOTAL'
        )
        print('DynamoDB write capacity used: ', response['ConsumedCapacity'])

def load_new_items():
    items = {}
    for feed in FEEDS:
        print(feed)
        try:
            resp = requests.get(feed, timeout=10.0)
        except requests.ReadTimeout:
            print("Timeout when reading RSS %s", feed)
            continue

        # Put it to memory stream object universal feedparser
        content = BytesIO(resp.content)

        # Parse content
        news_feed = feedparser.parse(content)

        #news_feed = feedparser.parse(feed)
        keys = []
        id_dedup = {}
        epoch_time = int(time.time()) + 2592000 * 36 # 3 years
        for entry in news_feed['entries']:
            id = entry['title_detail']['value'].lower()
            if id in id_dedup:
                continue
            id_dedup[id] = id
            keys.append({'id': id})
            items[id] = {
                'id': id,
                'expire': epoch_time,
                'message': entry['title_detail']['value'] + ': ' +  entry['link']
            }
            if len(keys) == 20:
                items = check_items(keys, items)
                keys = []

        if len(keys) != 20:
            items = check_items(keys, items)

    dynamodb_items = []
    for __, value in items.items():
        dynamodb_items.append({'PutRequest': {'Item': value}})
        if len(dynamodb_items) == 20:
            commit_items(dynamodb_items)
            dynamodb_items = []
    commit_items(dynamodb_items)
    return items


def post_message(content, tries=0):
    if tries > 5:
        print("DEBUG: Retried 6 times. Giving up")
        return False
    response = requests.post(url=HOOK_URL, json={"Content": content})
    if response.status_code != 200:
        print("DEBUG: Content: ", content)
        print("DEBUG: Response: ", response)
        print("DEBUG: Response: ", response.reason)
        print("DEBUG: Going to 3 second sleep")
        time.sleep(3)
        print("DEBUG: Retrying")
        return post_message(content, tries + 1)
    else: time.sleep(1)
    return True

def lambda_handler(event, context):
    new_messages = load_new_items()
    status = False
    content = ""
    if len(new_messages) >= 60:
        response = requests.post(url=HOOK_URL, json={"Content": "Too many new messages... Check the site: https://aws.amazon.com/new. "})
        print(response)
        return

    for __, message in new_messages.items():
        content = message['message']
        if post_message(content) != True:
            print("DEBUG: Failed to send: ", message['id'])
            status = True
    
    if status:
        raise Exception("At least one news wasn't sent!")
