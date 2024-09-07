import json
import pandas as pd
import boto3
from io import StringIO
import re

def lambda_handler(event, context):
    
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    source_key = event['Records'][0]['s3']['object']['key']
    
    
    # Set up S3 client
    s3_client = boto3.client('s3')
    
    try:
        # Read the file from the source bucket
        response = s3_client.get_object(Bucket=source_bucket, Key=source_key)
        file_content = response['Body'].read().decode('utf-8')
        
        # Load data into a DataFrame
        df = pd.read_json(StringIO(file_content))
        
        # Data cleaning and transformation
        df = clean_and_transform_data(df)
        
        # Data validation
        df = validate_data(df)
        
        # Convert to JSON
        json_buffer = StringIO()
        df.to_json(json_buffer, orient='records')
        
        # Upload to destination S3 bucket
        dest_bucket = 'transformeds3sql'
        dest_key = f'transformed_{source_key}'
        
        s3_client.put_object(Bucket=dest_bucket, Key=dest_key, Body=json_buffer.getvalue())
        
        return {
            'statusCode': 200,
            'body': json.dumps(f'Data transformed and uploaded to {dest_bucket}/{dest_key} successfully')
        }
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error processing file: {str(e)}')
        }

def clean_and_transform_data(df):
    # Handle missing values
    df['age'].fillna(df['age'].mean(), inplace=True)
    df['balance'].fillna(0, inplace=True)
    df['debt'].fillna(0, inplace=True)
    
    # Convert signup_date to datetime
    df['signup_date'] = pd.to_datetime(df['signup_date'])
    
    # Filter users by signup date (e.g., keep only users who signed up in the last year)
    one_year_ago = pd.Timestamp.now() - pd.DateOffset(years=1)
    df = df[df['signup_date'] > one_year_ago]
    
    # Compute age groups (5-year range)
    df['age_group'] = pd.cut(df['age'], bins=range(0, 101, 5), right=False, labels=[f"{i}-{i+4}" for i in range(0, 96, 5)])
    
    # Compute balance groups (50K rupees range)
    # Assuming 1 USD = 85 INR for this example
    df['balance_inr'] = df['balance'] * 85
    df['balance_group'] = pd.cut(df['balance_inr'], 
                                 bins=range(0, 10000001, 50000), 
                                 right=False, 
                                 labels=[f"{i//1000}K-{(i+50000)//1000}K" for i in range(0, 10000000, 50000)])
    
    df['signup_date'] = df['signup_date'].dt.strftime('%Y-%m-%d')
    return df

def validate_data(df):
    # Email format validation
    email_pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    df['valid_email'] = df['email'].str.match(email_pattern)
    
    # Age constraints (e.g., between 18 and 100)
    df['age_constraints'] = df['age'].apply(lambda x: 18 <= x <= 100)
    
    # Ensure balance is non-negative
    df = df[df['balance'] >= 0]
    
    # Ensure debt is non-negative
    df = df[df['debt'] >= 0]
    
    # Age group counts
    age_group_counts = df['age_group'].value_counts()
    age_group_counts_filtered = age_group_counts[age_group_counts > 0]

    # Balance group counts
    balance_group_counts = df['balance_group'].value_counts()
    balance_group_counts_filtered = balance_group_counts[balance_group_counts > 0]
    
    invalid_email_count = df['valid_email'].apply(lambda x: not x).sum()
    under_age_count = df['age'].apply(lambda x: x < 18).sum()

    # Log results in CloudWatch
    print("Age Group Counts:")
    print(age_group_counts_filtered.to_dict())
        
    print("Balance Group Counts:")
    print(balance_group_counts_filtered.to_dict())
    
    print("Invalid Email Count:")
    print(invalid_email_count)
        
    print("Under Age 18 Count:")
    print(under_age_count)
    
    return df