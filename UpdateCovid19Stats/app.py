import sys
import os
import json
import boto3
import botocore
import pandas as pd
from datetime import datetime

# Grab data from the environment.
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
TABLE_NAME = os.getenv('TABLE_NAME', 'covid19_stats')
NYT_CSV_URL = os.getenv('NYT_CSV_URL', 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv')
JH_CSV_URL = os.getenv('JH_CSV_URL', 'https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv')

ssm = boto3.client('ssm')

def lambda_handler(event, context):
    db = boto3.resource('dynamodb', region_name=AWS_REGION)
    table = db.Table(TABLE_NAME)
    try:
        checkpoint = ssm.get_parameter(Name='/covid19_stats/checkpoint')
        first_run = False
    except ssm.exceptions.ParameterNotFound:
        first_run = True
    
    df = merge(NYT_CSV_URL,JH_CSV_URL)
    load(df, first_run)

def merge(nyt_csv,jh_csv):
    nyt_df = extract_nyt_df(nyt_csv)
    jh_df = extract_jh_df(jh_csv)
    output_df = pd.merge(nyt_df,jh_df)
    return output_df
    
def extract_nyt_df(csv):
    dtypes = {'date': str, 'cases' : int, 'deaths' : int}
#    date_cols = ['date']
#    df = pd.read_csv(csv, dtype = dtypes, parse_dates = date_cols)
    df = pd.read_csv(csv, dtype = dtypes)
    df.rename(columns={'date':'Date'}, inplace=True)
    df.rename(columns={'cases':'Cases'}, inplace=True)
    df.rename(columns={'deaths':'Deaths'}, inplace=True)
    return df

def extract_jh_df(csv):
    dtypes = {'Date' : str, 'Recovered' : float}
#    date_cols = ['Date']
#    df = pd.read_csv(csv, dtype = dtypes, parse_dates = date_cols)
    df = pd.read_csv(csv, dtype = dtypes)
    df = df.iloc[[index for index,row in df.iterrows() if row['Country/Region'] == 'US']]
    df = df.filter(items = ['Date', 'Recovered'])
    df = df.astype({'Recovered': int})
    return df

def load(df,first_run):
    if first_run:
        checkpoint = '0000-00-00'
    else:
        checkpoint = ssm.get_parameter(Name='/covid19_stats/checkpoint')['Parameter']['Value']

    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(TABLE_NAME)

    with table.batch_writer() as batch:
        for index, row in df.iterrows():
            if validate_row(row):
                if row['Date'] > checkpoint:
                    checkpoint = row['Date']
                    output = row.to_dict() 
                    batch.put_item(Item=output)
            else:
                print('Row skipped: ',output)
    ssm.put_parameter(Name='/covid19_stats/checkpoint', Value=checkpoint, Type='String')

def validate_row(row):
    return(validate_date(row['Date']) and validate_number(row['Cases']) and validate_number(row['Deaths']) and validate_number(row['Recovered']))

def validate_date(date_text):
    try:
        datetime.strptime(date_text, '%Y-%m-%d')
        return(True)
    except ValueError:
        print('Incorrect date format, should be YYYY-MM-DD: ' + date_text)
        return(False)

def validate_number(input):
    try:
        val = int(input)
        val += 0
        return(True)
    except ValueError:
        print('Incorrect data value, expected non-negative integer: ' + input)
        return(False)
