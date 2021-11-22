from my_twint import twintv2 as twint
import pandas as pd
import numpy as np
from typing import Union, List


def tweets_by_keyword(keyword: str, start: str = None, end: str = None, limit: int = 100, to_csv: str = None, lang='en',
                      to_json: str = None, max_iter: int = 10, cols: List[str] = None) -> pd.DataFrame:
    if cols is None:
        cols = ['created_at', 'date', 'place', 'username', 'name', 'tweet', 'nlikes',
                'nreplies', 'nretweets']
    result = pd.DataFrame()
    t = twint.Config()
    t.Search = keyword
    t.Since = start
    t.Until = end
    t.Pandas = True
    t.limit = limit
    t.Lang = lang
    failed_counter = 0
    cols = cols if cols else ['created_at', 'date', 'place', 'username', 'name', 'tweet', 'nlikes',
                              'nreplies', 'nretweets']
    while len(result) < limit:
        try:
            twint.run.Search(t)
            df = twint.storage.panda.Tweets_df
            result = result.append(df[cols])
        except:
            failed_counter += 1
            if failed_counter >= max_iter:
                raise ValueError(f"Repeated {max_iter} times, no result is found. "
                                 f"Try upgrade twintv2 version or different IP address")
    if to_csv:
        result.to_csv(path_or_buf=to_csv, index=True)
    if to_json:
        result.to_json(path_or_buf=to_json, orient='records')
    return result


if __name__ == '__main__':
    print(tweets_by_keyword('iphone', '2020-01-01', '2020-01-02', limit=100))