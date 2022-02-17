# trying to find producers bc the db might be available 

import pandas as pd

from math import prod
from imdb import IMDb, helpers
from tqdm import tqdm 
from producer_eps import get_episodes
# # create an instance of the IMDb class
ia = IMDb('s3', 'postgresql://ivan:password@localhost/imdb')
df = pd.read_csv('titleepisode.tsv', sep='\t')
df_basics = pd.read_csv('titlebasics.tsv', sep='\t')
df = df.merge(df_basics, on='tconst', how='inner')
crew_df =  pd.read_csv('titleprincipals.tsv', sep='\t')
print(crew_df.columns)
crew_df = crew_df.loc[crew_df['job'] == "producer"]
crew_df = crew_df[['tconst', 'nconst']]

ep_df = pd.read_csv("titleepisode.tsv", sep='\t')

def get_parent_show_ids(df, seasons, testYear):
    parent_show_ids = set()
    df = df.replace("\\N",0)
    with tqdm(total=len(df)) as pbar:
        iter = 0
        for row in df.itertuples():
            iter +=1  
            testYear = max(int(row.startYear), int(row.endYear))
            season = int(row.seasonNumber)
            if (season>=seasons) and (testYear >=2021):
                parent_show_ids.add(row.parentTconst)
            pbar.update(iter)
    print(len(parent_show_ids))
    return parent_show_ids
    ### this just a set of ids

def get_episode_ids(series_set, ep_df):
    ep_by_series = ep_df.groupby("parentTconst")
    target_eps = pd.DataFrame()
    for name, group in ep_by_series:
        if name in series_set:
            target_eps = pd.concat([target_eps, group])
    return target_eps
   # this is just a dataframe of episodes from the relevant series 

def get_producers(series_set, crew_df):
    crew_by_series = crew_df.groupby('tconst')
    producer_df = pd.DataFrame()
    for name, group in crew_by_series:
        if name in series_set:
            producer_df = pd.concat([producer_df, group])        
    return producer_df
    # this adds producers from the crew credits to a df if the parent series is found in the crew credits

def assign_eps(producer_df, target_ep_df):
    eps_by_series = target_ep_df.groupby('parentTconst')
    producers_by_series = producer_df.groupby('tconst')
    for ep_series, episodes in eps_by_series:
        for prod_series, producers in producers_by_series:
            if ep_series == prod_series:
                producers_list = list(producers.unique())
                for row in episodes.itertuples()[::-1]:
                    pass
    
parent_show_ids = get_parent_show_ids(df, 2, 2021)
target_episodes = get_episode_ids(parent_show_ids, ep_df)
producers = get_producers(parent_show_ids, crew_df)

print(len(target_episodes))
print(target_episodes.head())
#pd.to_pickle(producer_df, "long_series_producers_s3.pkl")

# how does the flow work?
# Step 0 get the full credits df for producers and associated tconst? maybe not parentTconst
# Step 1 get the parent show ids using sets
# Step 2 get the episode ids from the paren
# Step 3 iterate through episode tconst to find latest observation for the producers
#   i. determine season and ep number through the iteration
#   ii. set up the dict from  
   
