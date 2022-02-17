### looking into the final producer database

import pandas as pd
from imdb import IMDb
import asyncio
from concurrent.futures import ThreadPoolExecutor

def get_name_from_id(id, column, ia):
    print(column, id)
    try:
        if column == 'personID':
            name = str(ia.get_person(id))
            print("sucessfully converted {} to {}".format(id,name))
            return name
        elif column == 'parentSeries':
            name = str(ia.get_movie(id))
            print("sucessfully converted {} to {}".format(id,name))
            return name
        elif column == 'lastEpisode':
            episode = ia.get_movie(id)
            title = str(episode['title'])
            print("sucessfully converted {} to {}".format(id, title))
            return title
        else:
            print("no parsable column passed")
            pass
    except Exception as e:
        print(str(e))
        print("failed on converting {} of type {}".format(id, column)) 
        print(id)
        pass

def df_apply(col, df, ia):
    df[col] = df[col].apply(lambda x: get_name_from_id(x, col, ia))


async def start_async_process(columns, df, ia, df_apply):
    with ThreadPoolExecutor(max_workers=10) as executor:
        loop = asyncio.get_event_loop()
        tasks = []
        iter = 0
        for col in columns:
            iter +=1
            print("working on group {} of {}".format(col, len(columns)))
            task = loop.run_in_executor(
                executor,
                df_apply,
                *(col, df, ia)   
            )
            tasks.append(task)
        for response in await asyncio.gather(*tasks):
            pass

def main():
    df = pd.read_pickle('final_producers.pkl')
    ia = IMDb()
    print(df.head())
    print(len(df))
    df['lastSeason'] = pd.to_numeric(df['lastSeason'])
    df = df.loc[df['lastSeason'] >= 2]
    print(len(df))
    cols = df.columns

    loop = asyncio.get_event_loop()
    future = asyncio.ensure_future(start_async_process(cols, df, ia, df_apply))
    loop.run_until_complete(future)
    print("finished collecting producer credits", len(df))
    df.to_pickle("final_producers_human.pkl")

if __name__ == "__main__":
    main()