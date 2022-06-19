'''
	This is the NHL crawler.  

Scattered throughout are TODO tips on what to look for.

Assume this job isn't expanding in scope, but pretend it will be pushed into production to run 
automomously.  So feel free to add anywhere (not hinted, this is where we see your though process..)
    * error handling where you see things going wrong.  
    * messaging for monitoring or troubleshooting
    * anything else you think is necessary to have for restful nights
'''

from io import BytesIO
from typing import List
from ratelimit import limits, RateLimitException
from backoff import on_exception, expo
from concurrent.futures import ThreadPoolExecutor

from botocore.config import Config
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from requests.exceptions import HTTPError, ConnectTimeout, Timeout
import pandas as pd
import requests
import boto3

import argparse
import logging
import yaml
import os

logging.basicConfig(level = logging.INFO)
logger = logging.getLogger(__name__)

JOBS_DIR = './nhldata/jobs'
API_CALL_LIMIT = 100
API_PERIOD_LIMIT = 10 # Seconds
API_MAX_RETRIES = 5
API_LIMIT_EXCEPTIONS = (RateLimitException, HTTPError, ConnectTimeout, Timeout)

class NHLApi:
    def __init__(self, api_host, api_version):
        self.base = '{}/{}'.format(api_host, api_version)

    def schedule(self, start_date: datetime, end_date: datetime) -> dict:
        """
        Fetches all nhl games between start_date and end_date from schedule endpoint

        :param start_date
        :param end_date
        :return: returns games json response as dict
        """
        start_date = start_date.strftime('%Y-%m-%d')
        end_date = end_date.strftime('%Y-%m-%d')
        
        logger.info('Fetching nhl games from {} to {}...'.format(start_date, end_date))
        
        url = self._url('schedule')
        params = {'startDate': start_date, 'endDate': end_date}
        response = self._get(url, params)

        logger.info('Successfully fetched nhl games from {} to {}'.format(start_date, end_date))
        
        return response

    def boxscore(self, game_id:int) -> dict:
        """
        Fetches box score stats and other metadata from boxscore endpoint for specific game_id

        :param game_id
        :return: returns boxscore json response as dict
        """
        logger.info('Fetching boxscore for game {}...'.format(game_id))
        
        url = self._url(f'game/{game_id}/boxscore')
        response = self._get(url)
        
        logger.info('Successfully fetched boxscore for game {}...'.format(game_id))

        return response

    @on_exception(expo, API_LIMIT_EXCEPTIONS, max_tries=API_MAX_RETRIES)
    @limits(calls=API_CALL_LIMIT, period=API_PERIOD_LIMIT)
    def _get(self, url, params=None):
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def _url(self, path):
        return f'{self.base}/{path}'

@dataclass
class StorageKey:
    game_id: str

    def key(self):
        ''' renders the s3 key for the given set of properties '''
        return f'{self.game_id}.csv'

class Storage:
    def __init__(self, endpoint_url: str, sig_version: str = 's3v4', num_retries: int = 5):
        self.endpoint_url = endpoint_url
        self.sig_version = sig_version
        self.num_retries = num_retries
        
        config = Config(signature_version = self.sig_version)
        self._s3_client = boto3.client('s3', config = config, endpoint_url = endpoint_url)

    def store_game(self, bucket:str, key: StorageKey, game_data: dict) -> None:
        """
        Uploads file object of game data to s3 bucket using key
        
        :param bucket: bucket to upload to
        :param key: s3 key for file
        :param game_data: bytes file object of game data
        """

        for _ in range(self.num_retries):
            try:
                logger.info('Uploading file to s3: {}/{}'.format(bucket, key.key()))
                self._s3_client.put_object(Bucket=bucket, Key=key.key(), Body=game_data)
                logger.info('Successfully uploaded file to s3: {}/{}'.format(bucket, key.key()))
                return
            except Exception as exp:
                logger.error('Error uploading file: {}. Retrying...'.format(exp))
                continue
        
        logger.error('Failed to upload file to s3: {}/{}'.format(bucket, key.key()))
        raise exp

class Crawler:
    def __init__(self, **kwargs):   
        self.player_fields = kwargs['fields']['player']
        self.stats_fields = kwargs['fields']['skater_stats']
        self.player_prefix = kwargs['prefixes']['player']
        self.stats_prefix = kwargs['prefixes']['stats']

        self.api = NHLApi(api_host = kwargs['api_host'],
                          api_version = kwargs['api_version'])
        
        self.storage = Storage(os.environ.get('S3_ENDPOINT_URL'))

    def get_games(self, start_date: datetime, end_date: datetime) -> List:
        """
        Fetches all nhl games played (game ids/pks) between start_date and end_date 
        
        :param start_date
        :param end_date
        :return: list of game ids/pks
        """
        
        # Fetch nhl games from schedule endpoint
        sched = self.api.schedule(start_date, end_date)
        dates = sched['dates']

        # For each date, grab games played on that date,
        # append to full games list
        game_pks = []
        for date in dates:
            games_on_date = [x['gamePk'] for x in date['games']]
            game_pks += games_on_date
        
        num_games = len(game_pks)
        logger.info('Found {} total games between {} and {}'.format(num_games, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))
        
        return game_pks

    def _gen_player_fields(self):
        """
        Generates full field names for player fields using player prefix

        :return: list of full player fields names
        """
        player_fields = [self.player_prefix + x for x in self.player_fields]
        return player_fields

    def _gen_stats_fields(self):
        """
        Generates full field names for stats fields using stats prefix

        :return: list of full stats fields names
        """
        stats_fields = [self.stats_prefix + x for x in self.stats_fields]
        return stats_fields

    def _gen_fields(self):
        """
        Generates all full field names using prefixes

        :return: list of all full field names
        """
        p_fields = self._gen_player_fields()
        s_fields = self._gen_stats_fields()
        return p_fields + s_fields

    def _convert_to_df(self, players: dict, fields: List[str], 
                       side: str, max_level: int) -> pd.DataFrame:
        """
        Converts semi-structered dict to dataframe with fields of interest

        :param players: dict of nested players fields
        :param fields: list of fields to grab
        :param side: home or away
        :param level: nested level of deepest fields
        :return: df 
        """
        try:
            logger.info('Converting data to df...')
            # Flatten json up to max_level
            df = pd.json_normalize(players, max_level = max_level)
            
            # Only grab fields + rename
            df = df[fields]
            df = df.rename(columns=lambda col: col.replace('.', '_'))
            df = df.rename(columns=lambda col: 'player_' + col)

            # Add home or away
            df['side'] = side
            
            logger.info('Successfully converted {} data to df'.format(side))
            return df
        
        except Exception as exp:
            logger.error('Failed to convert to df with error {}'.format(str(exp)))
            raise exp

    def _df_to_fileobj(self, df: pd.DataFrame, encoding: str = 'utf-8') -> BytesIO:
        """
        Converts df to csv bytes file object in memory
        
        :param df: dataframe
        :return: csv file object
        """
        try:
            df.fillna('', inplace=True)
            logger.info('Converting df to file object...')
            
            csv_bytes = df.to_csv(index=False).encode(encoding)
            file_obj = BytesIO(csv_bytes)
            
            logger.info('Successfully converted df to file object...')
            return file_obj
        except Exception as exp:
            logger.error('Failed to convert df to file object with error: {}'.format(str(exp)))
            raise exp

    def game_fetch_upload(self, game_pk: int, fields: List[str], 
                          max_level: int, bucket: str):
        """
        Fetches nhl game data for specific game id/pk from api, grabs only 
        fields of interest and uploads to s3
        
        :param game_pk: game id
        :param fields: list of fields to grab
        :max_level: nested level of deepest fields
        :bucket: s3 bucket to upload to
        """
        try:
            box_score = self.api.boxscore(game_pk)

            # Get home and away players data
            away_players = list(box_score['teams']['away']['players'].values())
            home_players = list(box_score['teams']['home']['players'].values())
            
            # Convert json to df
            away_df = self._convert_to_df(away_players, fields, side='away', max_level=max_level)
            home_df = self._convert_to_df(home_players, fields, side='home', max_level=max_level)
            
            # Combine home and away players data
            df = pd.concat([away_df, home_df])

            # Add game id and insert timestamp
            df['game_id'] = game_pk
            df['insert_timestamp'] = datetime.now(timezone.utc)
            
            # Convert df to csv file object
            file_obj = self._df_to_fileobj(df)

            # Upload to s3
            key = StorageKey(game_pk)
            self.storage.store_game(bucket, key, file_obj)
            self.success_cnt += 1
        except:
            pass

    def crawl(self, bucket, start_date: datetime, end_date: datetime, 
              max_level:int = 3, max_workers:int = 3) -> None:
        """
        Crawls all nhl game data between start_date and end_date and uploads to s3 in
        individual files. Multi-threaded (thread per game).
        
        :param bucket: s3 bucket to upload to
        :param start_date
        :param end_date
        :max_level: nested level of deepest fields
        :max_workers: number of threads
        """
        game_pks = self.get_games(start_date, end_date)
        fields = self._gen_fields()

        self.success_cnt = 0
        with ThreadPoolExecutor(max_workers = max_workers) as executor:
            logger.info('Starting ThreadPoolExecutor with {} workers...'.format(max_workers))
            for game_pk in game_pks:
                executor.map(self.game_fetch_upload(game_pk, fields, max_level, bucket))
        
        logger.info('Succesfully fetched and uploaded {} out of {} games'.format(self.success_cnt, len(game_pks)))
        logger.info('Completed job from {} to {}'.format(start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')))

        # If success count doesn't match games returned from api, 
        # raise exception
        if self.success_cnt != len(game_pks):
            fail_cnt = len(game_pks) - self.success_cnt
            logger.error('{} games failed to fetch and upload'.format(fail_cnt))
            raise Exception('Game ids returned != game ids uploaded')

class CrawlerJob:
    def __init__(self, job_config):
        self.job_config = job_config
        self.job_config['backfill'] = self.job_config.get('backfill', False)
        
        self.crawler = Crawler(fields = job_config['fields'],
                               prefixes = job_config['prefixes'],
                               api_host = job_config['api_host'],
                               api_version = job_config['api_version'])
    
    def run(self):
        """
        Runs a crawl job using a job yml
        """
        
        # If backfill job, grab dates defined in job yml
        if self.job_config['backfill']:
            logger.info('Starting backfill job...')
            self.job_config['start_date'] = datetime.strptime(self.job_config['start_date'], '%Y-%m-%d')
            self.job_config['end_date'] = datetime.strptime(self.job_config['end_date'], '%Y-%m-%d')
            
            self.crawler.crawl(self.job_config['s3_bucket'],
                               self.job_config['start_date'], 
                               self.job_config['end_date'],
                               self.job_config['max_level'],
                               self.job_config['max_workers'])
        
        # Otherwise, must be incremental job, 
        # use lookback days for dynamic dates
        else:
            curr_date = datetime.today()
            lookback_date = datetime.today() - timedelta(days=self.job_config['lookback_days'])
            
            logger.info('Starting incrental job...')
            self.crawler.crawl(self.job_config['s3_bucket'],
                               lookback_date, 
                               curr_date,
                               self.job_config['max_level'])

def main():
    parser = argparse.ArgumentParser(description='NHL Stats crawler')
    parser.add_argument('--job_yml', default='backfill_job.yml')

    args = parser.parse_args()
    job_yml_path = '{}/{}'.format(JOBS_DIR, args.job_yml)

    config_file = open(job_yml_path, 'r')
    job_yml = yaml.safe_load(config_file)
    
    crawler_job = CrawlerJob(job_yml)
    crawler_job.run()

if __name__ == '__main__':
    main()
