#### look at report 
import pandas as pd
import xlsxwriter 
df = pd.read_pickle('final_producers_human.pkl')
df2 = pd.read_pickle('final_producers.pkl')
df['lastSeason'] = df2['lastSeason']
df['lastYear'] = df2['lastYear']
print(df.head())

df.to_excel("ContinuingProducers.xlsx")  