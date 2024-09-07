import pymysql
import boto3
import json
import os
import pandas as pd
from datetime import datetime

# Configuration for connecting to the RDS or MySQL instance
RDS_HOST = os.getenv('RDS_HOST')
RDS_USER = os.getenv('RDS_USER')
RDS_PASSWORD = os.getenv('RDS_PASSWORD')
RDS_PORT = int(os.getenv('RDS_PORT', 3306))  # Default MySQL port

# Configuration for S3
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
S3_FILE_KEY = os.getenv('S3_FILE_KEY', 'data.json')  # The name of the file in S3
S3_FILE_KEY= f"{S3_FILE_KEY}_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"

def lambda_handler(event, context):
    connection = None
    try:
        # Establish connection to MySQL server
        connection = pymysql.connect(
            host=RDS_HOST,
            user=RDS_USER,
            password=RDS_PASSWORD,
            port=RDS_PORT,
            database='dev1'  # Ensure the database is specified
        )

        with connection.cursor(pymysql.cursors.DictCursor) as cursor:
            # Query data
            cursor.execute("""
                SELECT u.user_id, u.name, u.email, u.age, u.signup_date,
                       b.account_number, b.balance, b.debt, b.address
                FROM users u
                JOIN bank_accounts b ON u.user_id = b.user_id
            """)
            rows = cursor.fetchall()

        # Convert query result to DataFrame
        df = pd.DataFrame(rows)

        # Convert 'signup_date' to the desired format
        if 'signup_date' in df.columns:
            df['signup_date'] = pd.to_datetime(df['signup_date']).dt.strftime('%Y-%m-%d')

        # Convert DataFrame to JSON
        json_data = df.to_json(orient='records', date_format='iso')

        # Upload JSON data to S3
        s3_client = boto3.client('s3')
        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=S3_FILE_KEY,
            Body=json_data,
            ContentType='application/json'
        )

        return {
            'statusCode': 200,
            'body': f'Successfully uploaded data to {S3_BUCKET_NAME}/{S3_FILE_KEY}'
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }

    finally:
        # Ensure the connection is closed if it was successfully created
        if connection is not None:
            connection.close()
