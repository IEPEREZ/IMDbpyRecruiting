# starting from the producers pickle lets see if we can compartmentalize
# the rest of the imdb calls to get a full list

import pandas as pd
from imdb import IMDb, helpers
import asyncio
from concurrent.futures import ThreadPoolExecutor
import logging

def get_episodes(df, _season_ep_list_dict):
    """
    to a dataframe that contains the columns ['personID', 'prodSeriesObj', 'movieID'] 
    adds the column ['episodes'] that contains a dictionary with keys = season_number, 
    value = list of movie objects 
    
    Parameters
    ----------
        df : pd.Dataframe object
            dataframe that contains the columns ['personID', 'prodSeriesObj', 'movieID'] 
        
        _season_ep_list : list of dict
            list of dictionaries that contain entries for each producer.
    
    Returns
    -------
        _season_ep_list : list of dict
            list of dictionaries that contain entries for each producer.

    Notes
    -----
        maybe not super necessary that it be a df iter tupes sort of deal when it can be a regular function
        ? idk 
    """
    #producers = df.personID.unique()
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
    """
    function that manages parallelization of API calls that can be generated for
    an iterable list of dfgroupby objects.
    
    Parameters
    ----------
        dfgroupby : pd.groupby object
            ?
        
        _season_ep_list_dict : dict
            ?
        

    Returns
    -------
        None 
    
    Notes
    -----
        Maybe generalize this function for any kind of api call?
    """
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

def main():
    """
    Main Function,
    The sequence is:
    1. initialzie env
        a. create inst ance of IMDb() as ia
        b. load data from longseriesproducers.pkl
    
    2. 

    """

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
