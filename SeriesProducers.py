# request
# this document will retrieve producers on television shows that participate in the second season onwards

from math import prod
from imdb import IMDb, helpers

# # create an instance of the IMDb class
ia = IMDb()#('s3', 'postgresql://ivan:password@localhost/imdb')

import pandas as pd
from tqdm import tqdm 

def import_data():
    """
    imports data FROM IMDB tsv's 'titleepisode', 'titlebasics'. and inner joins them
    on tconst so that we have a final database with columns
    ['']
    
    Parameters
    ----------
        None
        
    Returns
    -------
        df : pd.Dataframe
            pd.Dataframe object of the merged dataframe.
    
    Raises
    ------
        None
    """
    df = pd.read_csv('titleepisode.tsv', sep='\t')
    df_basics = pd.read_csv('titlebasics.tsv', sep='\t')
    df = df.merge(df_basics, on='tconst', how='inner')
    print(df.columns)
    return df 

def get_parent_show_ids(df, min_seasons, testYear):
    """
    gets parent show for shows that satisfy mininmum number 
    of seasons and started or ended in the testYear.
    
    Parameters
    ----------
        df : pd.DataFrame object
            the merged df from load data function
            requres columns 'startYear', 'endYear', 'seasonNumber', 'parentTconst'
        
        min_seasons : int
            minimum number of seasons of interest in the show 
        
        testYear : int
            lower bound of the start or end of the series we are looking for.
        
    Returns
    -------
        parent_show_ids : set of str
            set of unique parent show tconst 
    
    Notes
    -----
        To generalize you should just have kwargs and pass them along

    """
    parent_show_ids = set()
    df = df.replace("\\N",0)
    with tqdm(total=len(df)) as pbar:
        iter = 0
        for row in df.itertuples():
            iter +=1 
            testYear = max(int(row.startYear), int(row.endYear))
            season = int(row.seasonNumber)
            if (season>=min_seasons) and (testYear >=2021):
                parent_show_ids.add(row.parentTconst)
            pbar.update(iter)
    print(len(parent_show_ids))
    return parent_show_ids


def get_producers(parent_show_ids, testbreak=10):
    """
    creates a dataframe that collects the producers for a set/iterable of parent show ids.
    The output dataframe has columns [personID, personObj, movieID, prodSeriesObj]
    
    Parameters
    ----------
        parent_show_ids : set
            set of parent show ids, parentTconst or tconst
        
        testbreak : int
            Default 10, number of shows to iterate through for testing purposes.
    
    Returns
    -------
        prod_df :pd.Dataframe
            * dataframe outlining all the producers of a show

    Notes
    -----
        not modular enough to do all roles 
    """
    prod_dict_list = []
    iter2 = 0 
    for element in parent_show_ids:
        print("we are ",iter2/len(parent_show_ids), "percent there")
        try:
            series = ia.get_movie(element[2:]) # imdb call 
            ia.update(series, 'full credits') # imdb call
            if 'producer' in series.keys():
                for member in series['producer']:
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
        if iter2 == testbreak:
            break
    prod_df = pd.DataFrame(prod_dict_list)
    return prod_df


def main():
    """
    main function
    the sequence is
    1. load data
    2. get the parent show ids that pass the filte
    3. get the producers for the set of parent show ids
    4. save the producer dataframe to a pickle
    """
    df = import_data()
    parent_ids = get_parent_show_ids(df,2,2021)
    producer_df = get_producers(parent_ids,0)
    pd.to_pickle(producer_df, "long_series_producers.pkl")
if __name__ == "__main__":
    main()
    