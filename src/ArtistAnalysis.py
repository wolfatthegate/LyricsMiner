import pymongo
import pandas as pd

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
twitterDB = myclient['TwitterData']

monthly_tables = [['2016-01-week-2', '2016-01-week-3', '2016-01-week-4'],
['2016-02-week-1', '2016-02-week-2', '2016-02-week-3'],
['2016-03-week-2', '2016-03-week-3', '2016-03-week-4'],
['2016-04-week-1', '2016-04-week-2', '2016-04-week-3', '2016-04-week-4'],
['2016-05-week-1', '2016-05-week-2', '2016-05-week-3', '2016-05-week-4'],
['2016-06-week-1', '2016-06-week-2', '2016-06-week-3', '2016-06-week-4'],
['2016-07-week-1', '2016-07-week-2', '2016-07-week-3', '2016-07-week-4'],
['2016-08-week-1', '2016-08-week-2', '2016-08-week-3', '2016-08-week-4'],
['2016-09-week-1', '2016-09-week-2', '2016-09-week-3', '2016-09-week-4'],
['2016-10-week-1', '2016-10-week-2', '2016-10-week-3', '2016-10-week-4'],
['2016-11-week-1', '2016-11-week-2', '2016-11-week-3', '2016-11-week-4'],
['2016-12-week-1', '2016-12-week-2', '2016-12-week-3', '2016-12-week-4'],
['2017-01-week-1', '2017-01-week-2', '2017-01-week-3', '2017-01-week-4'],
['2017-02-week-1', '2017-02-week-2', '2017-02-week-3', '2017-02-week-4'],
['2017-03-week-1', '2017-03-week-2', '2017-03-week-3', '2017-03-week-4'],
['2017-04-week-1', '2017-04-week-2', '2017-04-week-3', '2017-04-week-4'],
['2017-05-week-1', '2017-05-week-2', '2017-05-week-3', '2017-05-week-4'],
['2017-06-week-1', '2017-06-week-2', '2017-06-week-3', '2017-06-week-4'],
['2017-07-week-1', '2017-07-week-2', '2017-07-week-3', '2017-07-week-4'],
['2017-08-week-1', '2017-08-week-2', '2017-08-week-3', '2017-08-week-4'],
['2017-09-week-1', '2017-09-week-2', '2017-09-week-3'],
['2017-10-week-1', '2017-10-week-2', '2017-10-week-3', '2017-10-week-4'],
['2017-11-week-1', '2017-11-week-2', '2017-11-week-3', '2017-11-week-4'],
['2017-12-week-1', '2017-12-week-2', '2017-12-week-3', '2017-12-week-4']]

for monthly_table in monthly_tables:
    frame = []
    
    for weekly_table in monthly_table: 
        tweetTbl = twitterDB[weekly_table]
        cursor = tweetTbl.find({'score': {'$gt': 0.49}})
        
        df =  pd.DataFrame(list(cursor))
        frame.append(df)
    
    monthly_result = pd.concat(frame)
    
    count = monthly_result.groupby(['artist', 'song'])['song'].count().reset_index(name='Count').sort_values(['Count'], ascending=False)  
    count.to_csv(weekly_table[0:7]+'_group_by_song.csv', index=True)
    
    count = monthly_result.groupby(['artist'])['artist'].count().reset_index(name='Count').sort_values(['Count'], ascending=False)
    count.to_csv(weekly_table[0:7]+'_group_by_artist.csv', index=True)
    
    print('{} finished'.format(weekly_table))
