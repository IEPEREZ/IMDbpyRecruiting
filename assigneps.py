# starting from a db that has producers and their show and a dict of seasons and eps
# lets start from the sorted episodes and calling imdb to give me full cast

import pandas as pd
from imdb import IMDb, helpers
import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging
import time
from random import randint
def assign_ep(df, producer_entry):
    """
    takes an existing dataframe with columns 
    []
    and looks through the episodes of the parent series associated with the producer.
    assigns the latest episode to the producer for whatever show they worked on. 
    
    Parameters
    ----------
        df : pd.Dataframe object

        producer_entry : list of dict
        
    Returns
    -------
        producer_entry : list_of_dict
    
    Notes
    -----
        lord only knows
    """
    producers = list(df.personID.unique())
    series = df['prodSeries'].iloc[0]
    sorted_seasons = df['episodes'].iloc[0]
    episode, year = None, None
    for key, value in reversed(sorted_seasons.items()):
        #print("looking at season {}".format(key))
        sorted_episodes = value
        for ep in sorted_episodes[::-1]:
            #print("looking at episode {}".format(str(ep['episode'])))
            if len(producers) > 0:
                try: 
                    ia.update(ep, 'full credits') #imdb call 
                    if 'producer' in ep.keys():
                        imdb_ep_producers = [str(p.personID) for p in ep['producer']]
                        ep_prods = [producer for producer in list(set(imdb_ep_producers).intersection(set(producers)))]
                        for ep_producer in ep_prods:
                            #print("person {} was a producer in {} on episodes {}".format(ep_producer, str(series.movieID), str(ep.movieID))) 
                            episode, year, season = str(ep.movieID), str(ep['year']), str(key)
                            _entry = {
                                'personID':ep_producer, 'parentSeries':str(series.movieID), 'lastSeason':season, 'lastEpisode':episode, 'lastYear':year
                                }
                            producer_entry.append(_entry)
                            producers.remove(ep_producer)
                    else:
                        print("producer category not foud") 
                except Exception as e:
                    print("error has occurred")
                    error = logging.exception("message")
                    if "503" in str(e): 
                        print("got a 503 fuck these people")
                        time.sleep(randint(300,900))
            else:
                print("all produceres found at episode level or list is empty") 
                break       
        if len(producers) == 0:
            print("all producers found at season level")
            break
    #print("sucessfully appened", _entry)
    return producer_entry
    


async def start_async_process(dfgroupby, producer_entry):
    with ThreadPoolExecutor(max_workers=10) as executor:
        loop = asyncio.get_event_loop()
        tasks = []
        iter = 0
        for group in dfgroupby:
            iter +=1
            print("working on group {} of {}".format(iter, len(dfgroupby)))
            df = group[1]
            task = loop.run_in_executor(
                executor,
                assign_ep,
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
