import pytest
import os
import json
from io import StringIO
import sys
sys.path.insert(0, os.path.abspath('UpdateCovid19Stats/'))
from app import all

NYT_CSV_URL = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv'
JH_CSV_URL = 'https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv'


def test_extract_NYT():
    assert type(extract(NYT_CSV_URL)) == StringIO

def test_extract_JH():
    assert type(extract(NYT_CSV_URL)) == StringIO
