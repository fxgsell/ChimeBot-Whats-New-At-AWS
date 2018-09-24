import os
import time
import feedparser
import boto3
import requests
import datetime
import json

from bs4 import BeautifulSoup

from datetime import timezone
from io import BytesIO

HOOK_URL = os.environ['BOT_URL']
TABLE_NAME = os.environ['TABLE_NAME']
FEEDS_TABLE = os.environ['FEEDS_CONFIG']
STREAM = os.environ['KINESIS_STREAM']

DB = boto3.resource('dynamodb')
KINESIS = boto3.client('firehose')
DEFAULT_FEEDS = [
    {'url': 'https://aws.amazon.com/new/feed/', 'source': 'AWS', 'category': 'news'},
    {'url': 'https://aws.amazon.com/security/security-bulletins/feed/', 'source': 'AWS', 'category': 'security'}    
]

def load_feeds():
    table = DB.Table(FEEDS_TABLE)
    response = table.scan()
    data = response['Items']
    while response.get('LastEvaluatedKey'):
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])
    print("Loaded feeds: ", data)
    return data

feeds = load_feeds()
if len(feeds) <= 0:
    feeds = DEFAULT_FEEDS

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

def clean_text(text):
    text = BeautifulSoup(text).get_text()
    text = text.replace('\\n', '\n')
    text = text.replace('\xa0', ' ')
    return text

def parse_date(date):
    date = date.replace(' +0000', '').replace(' -0000', '')
    date = date.replace(' Z', '')
    dt = datetime.datetime.strptime(date, "%a, %d %b %Y %H:%M:%S").isoformat()
    return dt
    
def load_new_items():
    items = {}
    for feed in feeds:
        print(feed)
        try:
            resp = requests.get(feed['url'], timeout=10.0)
        except requests.ReadTimeout:
            print("Timeout when reading RSS %s", feed)
            continue

        content = BytesIO(resp.content)
        news_feed = feedparser.parse(content)

        keys = []
        id_dedup = {}
        epoch_time = int(time.time()) + 2592000 * 36 # 3 years
        for entry in news_feed['entries']:
            title = entry['title']
            summary = clean_text(entry['summary'])
            dt = parse_date(entry['published'])

            if len(summary) <= 0:
                summary = title

            id = feed['source'] + " " + title.lower()
            if id in id_dedup:
                continue

            id_dedup[id] = id
            keys.append({'id': id})
            items[id] = {
                'id': id,
                'title': title,
                '@timestamp': dt,
                'source': feed['source'],
                'category': feed['category'],
                'summary': summary,
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
    else:
        time.sleep(1)
    return True

def publish(message):
    if len(STREAM) > 0:
        KINESIS.put_record(
            DeliveryStreamName=STREAM,
            Record={'Data': message.encode('utf-8')}
        )

def lambda_handler(event, context):
    new_messages = load_new_items()
    status = False
    content = ""
    if len(new_messages) >= 500:
        response = requests.post(url=HOOK_URL, json={"Content": "Too many new messages..."})
        print(response)
        return

    print("Posting {} news.".format(len(new_messages)))
    for __, message in new_messages.items():
        publish(json.dumps(message))
        content = '[' + message['source'] + '] ' + message['message']
        if post_message(content) != True:
            print("DEBUG: Failed to send: ", message['id'])
            status = True
    
    if status:
        raise Exception("At least one news wasn't sent!")
