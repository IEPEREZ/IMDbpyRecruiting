# starting from the producers pickle lets see if we can compartmentalize
# the rest of the imdb calls to get a full list

import pandas as pd
from imdb import IMDb, helpers
import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging

def get_episodes(df, _season_ep_list_dict):
    producers = df.personID.unique()
    series = df['prodSeriesObj'].iloc[0]
    try:
        ia.update(series, 'episodes') # imdb call    
        if 'episodes' in series.keys():
            seasonlist = series['episodes'].keys() 
            _season_dict = {}
            for seasons in seasonlist:
                print("looking into season {}".format(seasons))
                sorted_episodes = helpers.sortedEpisodes(series, season=int(seasons))
                print("there are:", len(sorted_episodes), " episodes")
                _season_dict.update({str(seasons) : sorted_episodes})
            for row in df.itertuples():
                _dict = {
                        'personID':row.personID,
                        'movieID':str(row.movieID),
                        'prodSeries':series,
                        'episodes': _season_dict
                    }
                _season_ep_list_dict.append(_dict)
        else:
            print("no episodes found for show {}".format(df['movieID'].iloc[0]))
    except:
        print("error has occurred")
        logging.exception("message")

async def start_async_process(dfgroupby, _season_ep_list_dict):
    with ThreadPoolExecutor(max_workers=20) as executor:
        loop = asyncio.get_event_loop()
        tasks = []
        iter = 0
        for group in dfgroupby:
            iter +=1
            print("working on group {} of {}".format(iter, len(dfgroupby)))
            df = group[1]
            task = loop.run_in_executor(
                executor,
                get_episodes,
                *(df, _season_ep_list_dict)   
            )
            tasks.append(task)
        for response in await asyncio.gather(*tasks):
            pass

# for group in by_series:
#     df = group[1]
#     _season_ep_list_dict = get_episodes(df, _season_ep_list_dict)

# ep_df = pd.DataFrame(_season_ep_list_dict)
# print(len(ep_df))
# pd.to_pickle(ep_df, "producer_episodes.pkl")

if __name__ == "__main__":
    ia = IMDb()
    prod_df = pd.read_pickle('long_series_producers.pkl')
    print(len(prod_df))
    onepct = int(0.01*len(prod_df)) 
    prod_df = prod_df[:onepct]
    _season_ep_list_dict = []
    by_series = prod_df.groupby('movieID')
    #print(prod_df['movieID'])
    print(len(by_series))
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(start_async_process(by_series,_season_ep_list_dict))
    loop.run_until_complete(future)
    ep_df = pd.DataFrame(_season_ep_list_dict)
    print(len(ep_df))
    pd.to_pickle(ep_df, "producer_episodes.pkl")
