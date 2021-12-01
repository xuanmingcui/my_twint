import time

from my_twint import twintv2 as twint
import pandas as pd
import numpy as np
from typing import Union, List
import datetime

from my_twint.twintv2.token import RefreshTokenException


def tweets_by_keyword(keyword: str, start: str = None, end: str = None, limit: int = 100, lang='en', to_csv: str = None,
                      to_json: str = None, max_iter: int = 10, cols: List[str] = None,
                      hide_output: bool = True) -> pd.DataFrame:
    result = pd.DataFrame()
    t = twint.Config()
    t.Search = keyword
    t.Since = start
    t.Until = end
    t.Pandas = True
    t.limit = limit
    t.Lang = lang
    failed_counter = 0
    t.Hide_output = hide_output
    cols = cols if cols else ['date', 'place', 'username', 'name', 'tweet', 'nlikes',
                              'nreplies', 'nretweets']
    while len(result) < limit:
        try:
            twint.run.Search(t)
            df = twint.storage.panda.Tweets_df
            if len(df) > 0:
                result = result.append(df[cols])
            failed_counter = 0
        except Exception as e:
            # if any error happens, rerun search; if rerun iterations > max_iter, break
            failed_counter += 1
            if failed_counter >= max_iter:
                raise e
    if to_csv:
        result.to_csv(path_or_buf=to_csv, index=True)
    if to_json:
        result.to_json(path_or_buf=to_json, orient='records')
    return result


def tweets_daily_by_keyword(keyword: str, start: str = None, end: str = None, daily_limit: int = 100, lang='en',
                            to_csv: str = None,
                            to_json: str = None, max_iter: int = 10, cols: List[str] = None,
                            hide_output: bool = True) -> pd.DataFrame:
    cols = cols if cols else ['date', 'place', 'username', 'name', 'tweet', 'nlikes', 'nreplies', 'nretweets']

    start_datetime, end_datetime = datetime.datetime.strptime(start, '%Y-%m-%d'), datetime.datetime.strptime(end,
                                                                                                             '%Y-%m-%d')
    result = pd.DataFrame(columns=cols)
    failed_counter = 0
    while start_datetime < end_datetime:
        print(f'Fetching result between {str(start_datetime)} and '
              f'{str(str(start_datetime + datetime.timedelta(days=1)))}')
        try:
            result = result.append(tweets_by_keyword(keyword=keyword, start=start_datetime.strftime('%Y-%m-%d'),
                                                     end=(start_datetime + datetime.timedelta(days=1)).strftime(
                                                         '%Y-%m-%d'),
                                                     limit=daily_limit, lang=lang, to_csv=None, to_json=None,
                                                     max_iter=max_iter, cols=cols, hide_output=hide_output))
            start_datetime += datetime.timedelta(days=1)
            failed_counter = 0
        except RefreshTokenException as e:
            failed_counter += 1
            if failed_counter > 3:
                raise e
            time.sleep(1500)
            pass

    if to_csv:
        result.to_csv(path_or_buf=to_csv, index=True)
    if to_json:
        result.to_json(path_or_buf=to_json, orient='records')
    return result


if __name__ == '__main__':
    df = tweets_daily_by_keyword('tsla OR TSLA or Tesla', '2018-01-01', '2021-11-01', daily_limit=100,
                                 to_csv='tsla_tweets.csv')
    print(len(df))
