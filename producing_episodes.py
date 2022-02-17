# starting from a db that has producers and their show and a dict of seasons and eps
# lets start from the sorted episodes and calling imdb to give me full cast

import pandas as pd
from imdb import IMDb, helpers
import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging

def get_producers(df, producer_entry):
    producers = list(df.personID.unique())
    #print(producers)
    series = df['prodSeries'].iloc[0]
    sorted_seasons = df['episodes'].iloc[0]
    episode, year = None, None
    for key, value in reversed(sorted_seasons.items()):
        print("looking at season {}".format(key))
        sorted_episodes = value
        for ep in sorted_episodes[::-1]:
            print("looking at episode {}".format(str(ep['episode'])))
            if len(producers) > 0:
                ia.update(ep, 'full credits') #imdb call 
                if 'producer' in ep.keys():
                    imdb_ep_producers = [str(p.personID) for p in ep['producer']]
                    ep_prods = [producer for producer in list(set(imdb_ep_producers).intersection(set(producers)))]
                    for ep_producer in ep_prods:
                        print("person {} was a producer in {} on episodes {}".format(ep_producer, str(series.movieID), str(ep.movieID))) 
                        #print("the producer was {} people love".format(ep['producer']))
                        episode, year, season = str(ep.movieID), str(ep['year']), str(key)
                        _entry = {
                            'personID':ep_producer, 'parentSeries':str(series.movieID), 'lastSeason':season, 'lastEpisode':episode, 'lastYear':year
                            }
                        producer_entry.append(_entry)
                        producers.remove(ep_producer)
                else:
                    print("producer category not foud") 
            else:
                print("all produceres found at episode level or list is empty") 
                break       
        if len(producers) == 0:
            print("producers found at season level")
            break
    print("sucessfully appened", _entry)
    
# we want a final db where 
# producer id, parentSieres, last_season, last_episode, last_year produced
# inevitably.... we will have entries with producer ids
# to aggregate producer credits for a producer id, we will make it a dict of the last stored
#  in prodFilmography 
# producer id, lastSeries, last_season, last_episode, last_year, prodFilmography

# we need to write an excel report function that outputs this db so that its parsable for ashley<3


async def start_async_process(dfgroupby, producer_entry):
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
                get_producers,
                *(df, producer_entry)   
            )
            tasks.append(task)
        for response in await asyncio.gather(*tasks):
            pass

if __name__ == "__main__":
    ia = IMDb()
    ep_df = pd.read_pickle('producer_episodes.pkl')
    print(len(ep_df))
    producer_entry = []
    by_series = ep_df.groupby('movieID')
    print(len(by_series))
    
    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(start_async_process(by_series,producer_entry))
    loop.run_until_complete(future)
    out_df = pd.DataFrame(producer_entry)
    print("finished collecting producer credits", len(out_df))
    out_df.to_pickle("final_producers.pkl")
