from datetime import datetime
import pytest
import json

from nhldata.app import NHLApi

API_HOST = 'https://statsapi.web.nhl.com/api/'
API_VERSION = 'v1'

TEST_RESOURCES = './tests/resources'

def save_json(json, name):
    with open('{}/{}'.format(TEST_RESOURCES, name), 'w', encoding='utf-8') as f:
        json.dump(json, f, ensure_ascii=False, indent=4)

def test_schedule():
    start_date = datetime(2020, 8, 4)
    end_date = datetime(2020, 8, 5)

    nhl_api = NHLApi(API_HOST, API_VERSION)
    res = nhl_api.schedule(start_date, end_date)
    del res["metaData"]
    
    f = open('{}/schedule_2020_08_04_to_2020_08_05.json'.format(TEST_RESOURCES))
    expected = json.load(f)
    assert res == expected

def test_boxscore():
    nhl_api = NHLApi(API_HOST, API_VERSION)
    res = nhl_api.boxscore(2019030042)
    
    f = open('{}/boxscore_2019030042.json'.format(TEST_RESOURCES))
    expected = json.load(f)
    assert res == expected
