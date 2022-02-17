# request
# this document will retrieve producers on television shows that participate in the second season onwards
# The steps we will take
# 1. find the set of television shows that have more than 1 season
# 1 alt. find the set of series that contain season objects of >1 same thing
# Then for each series I will look in season 2 and extract the episode objects
# from the episode objects I will extract producers and append to a list
# the final list should be 
# name, TV show, seasons, is also first season producer ? 

from math import prod
from imdb import IMDb, helpers

# # create an instance of the IMDb class
ia = IMDb('s3', 'postgresql://ivan:password@localhost/imdb')
# # get a movie
# movie = ia.get_movie('0133093')

# # print the names of the directors of the movie
# print('Directors:')
# for director in movie['directors']:
#     print(director['name'])

# # print the genres of the movie
# print('Genres:')
# for genre in movie['genres']:
#     print(genre)

# # search for a person name
# people = ia.search_person('Mel Gibson')
# for person in people:
#    print(person.personID, person['name'])

import pandas as pd
from tqdm import tqdm 

df = pd.read_csv('titleepisode.tsv', sep='\t')
df_basics = pd.read_csv('titlebasics.tsv', sep='\t')
df = df.merge(df_basics, on='tconst', how='inner')
#print(df.head())
print(df.columns)

#titlecrew doesn't work because it is just directors and writers.
# title episodes toget the ID for the parent TV show

#print(len(df))

parent_show_ids = set()
# this for thing gest the shows that had long seasons past 1
df = df.replace("\\N",0)
with tqdm(total=len(df)) as pbar:
    iter = 0
    for row in df.itertuples():
        iter +=1 
        testYear = max(int(row.startYear), int(row.endYear))
        season = int(row.seasonNumber)
        #if row.seasonNumber == "\\N":
        #    season=0
        #else:
        #    season = int(row.seasonNumber) 
        #if row.endYear == "\\N":
        #    endYear=0
        #else:
        #    endYear=int(row.endYear)    
        #testYear = max(int(row.startYear), endYear)
        if (season>1) and (testYear >=2021):
            parent_show_ids.add(row.parentTconst)
        pbar.update(iter)
print(len(parent_show_ids))
# removal_set= set()
# # getting only the set of shows that have happened in the last 10 years
# for id in parent_show_ids:
#     entry = df_basics[df_basics['tconst'] == id]  
#     startYear = int(entry['startYear'])
#     try:
#         endYear = int(entry['endYear'])
#     except:
#         print(entry['endYear'])
#         endYear = 0 
#     testYear=max(startYear, endYear)
#     if testYear not in range(2012, 2023, 1):
#         removal_set.add(id)

# parent_show_ids -= removal_set
# print(len(parent_show_ids))


# this loop should 
# 1. iterate through the show ids to get the series movie object
# 2. iterate through the show level credits to identify cast members
# 3. iterate through the filmography of each cast member to identify if
#    they were producers 
# 4. if the cast member was producer, we iterate through their producing
#    roles to see if they produced for the show in question
# 5. if they produced for the show in question we determine which
#    episodes they produced for.
# 6.  
prod_dict_list = []
#prod_df = pd.DataFrame(columns=['personID', 'personObj', 'prodSeries'], index=None)
#print(prod_df.head())
iter2 = 0 
for element in parent_show_ids:
    print("we are ",iter2/len(parent_show_ids), "percent there")
    try:
        series = ia.get_movie(element[2:]) # imdb call 
        ia.update(series, 'full credits') # imdb call
        if 'producer' in series.keys():
            for member in series['producer']:
                #print(type(p))
                #print(vars(p)) 
                    #p = ia.get_person(member.personID) # imdb call
                    #print(type(p2)) 
                    _entry_dict = {
                        'personID':str(member.personID),
                        'personObj': member,
                        'movieID': str(series.movieID),
                        'prodSeriesObj': series
                        }
                    prod_dict_list.append(_entry_dict)
                    print(len(prod_dict_list))
    except:
        print("timeout error")
        continue
    iter2+=1
    if iter2 == 10:
        break



prod_df = pd.DataFrame(prod_dict_list)
pd.to_pickle(prod_df, "long_series_producers_test.pkl")

"""
################################################            
            if matched_index_entry:
                ia.update(series, 'episodes') # imdb call
                seasonlist = series['episodes'].keys() 
                print(seasonlist) 
                for seasons in seasonlist:
                    print("looking into season {}".format(seasons))
                    sorted_episodes = helpers.sortedEpisodes(series, season=seasons)
                    print("there are:", len(sorted_episodes), " episodes")
                    ####
                    for ep in sorted_episodes:
                    #### lets stop here with a list of sorted episode objects in a dataframe
                    #    
                        ia.update(ep, 'full credits') #imdb call 
                        if str(p.personID) in ep['cast']:
                            print("person {} was a cast/producer in {} on episodes {}".format(str[p.personID], str(series.movieID), str(ep.movieID))) 
                        print("the cast list was {} people love".format(len(ep['cast'])))
                        #ep_list = p['episodes'][matched_index_entry[0]] # hopefully
                #print(ep_list)      
"""