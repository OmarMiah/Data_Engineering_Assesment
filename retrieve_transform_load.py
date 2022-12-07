# Extract data from sqs queue
import boto3
import localstack_client.session as boto3
import logging
from botocore.exceptions import ClientError
import json
import psycopg2 
import pandas as pd
import hashlib
from datetime import date
import json
import psycopg2

# logger config setup
logger = logging.getLogger()
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s: %(levelname)s: %(message)s')

# Constansts and Parameters for SQS Queue
QUEUE_NAME = "login-queue"
sqs = boto3.client('sqs')

# Receiving messages from SQS Queue
def receive_queue_message(queue_url):
    """
    Retrieves one or more messages (up to 10), from the specified queue.
    """
    try:
        response = sqs.receive_message(QueueUrl=queue_url)
    except ClientError:
        logger.exception(
            f'Could not receive the message from the - {queue_url}.')
        raise
    else:
        return response

# Deleting message from SQS Queue
def delete_queue_message(queue_url, receipt_handle):
    """
    Deletes the specified message from the specified queue.
    """
    try:
        response = sqs.delete_message(QueueUrl=queue_url,
                                             ReceiptHandle=receipt_handle)
    except ClientError:
        logger.exception(
            f'Could not delete the meessage from the - {queue_url}.')
        raise
    else:
        return response

# Driver
if __name__ == '__main__':
    
    # CONSTANTS
    QUEUE_URL = f'http://localhost:4566/000000000000/{QUEUE_NAME}'.format()
    df = pd.DataFrame() # empty dataframe

    # call to poll for messages in the queue
    messages = receive_queue_message(QUEUE_URL)

    # Append the message to a dataframe, log, and delete message
    for msg in messages['Messages']:
        msg_body = msg['Body']
        receipt_handle = msg['ReceiptHandle']
        result_dict = json.loads(msg_body)

        logger.info(f'The message body: {msg_body}')
        
        df = df.append(pd.json_normalize(result_dict), ignore_index=True)
            
        logger.info('Deleting message from the queue...')

        delete_queue_message(QUEUE_URL, receipt_handle)
      
    logger.info(f'Received and deleted message(s) from {QUEUE_URL}.')

    # Renaming columns to be masked in the dataframe
    df.rename(columns = {'ip':'masked_ip', 'device_id':'masked_device_id'}, inplace = True)

    # Convert column to string
    df['masked_ip'] = df['masked_ip'].astype(str)
    # Apply hashing function to the column to mask ip
    df['masked_ip'] = df['masked_ip'].apply(
        lambda x: 
            hashlib.sha256(x.encode()).hexdigest()
    )

    # Convert column to string
    df['masked_device_id'] = df['masked_device_id'].astype(str)
    # Apply hashing function to the column to mask device id
    df['masked_device_id'] = df['masked_device_id'].apply(
        lambda x: 
            hashlib.sha256(x.encode()).hexdigest()
    )

    # Setting pandas values to upload to SQL String
    user_id = df['user_id'].values[0]
    app_version = df['app_version'].values[0]
    device_type = df['device_type'].values[0]
    masked_ip = df['masked_ip'].values[0]
    locale = df['locale'].values[0]
    masked_device_id = df['masked_device_id'].values[0]
    create_date = date.today()

    # establishing the connection to Postgres DB
    conn = psycopg2.connect(
        database="postgres",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432",
    )

    # Creating a cursor object using the cursor() method
    conn.autocommit = True
    cursor = conn.cursor()

    # Issue uploading app_version so will convert column to a varchar
    sql_altertable = """ALTER TABLE user_logins ALTER COLUMN app_version TYPE varchar (255);"""
    cursor.execute(sql_altertable)

    sql_altertable2 = """ALTER TABLE user_logins ALTER COLUMN locale TYPE varchar (255);"""
    cursor.execute(sql_altertable2)

    sql_altertable3 = """ALTER TABLE user_logins ALTER COLUMN device_type TYPE varchar (255);"""
    cursor.execute(sql_altertable3)

    # # Inserting data into the user_logins table
    sql = f"""INSERT INTO user_logins(user_id, device_type, masked_ip, masked_device_id,
    locale, app_version, create_date) VALUES ('{user_id}' ,'{device_type}','{masked_ip}','{masked_device_id}','{locale}' ,'{app_version}','{create_date}')"""

    cursor.execute(sql)

    conn.commit()
    conn.close()