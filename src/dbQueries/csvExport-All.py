import sys
import pymongo
import pandas as pd

dbname = 'TwitterData' #TwitterData #tweet_drugs

# tableList = ['2016-01-week-2', '2016-01-week-3', '2016-01-week-4',
# '2016-02-week-1', '2016-02-week-2', '2016-02-week-3',
# '2016-03-week-2', '2016-03-week-3', '2016-03-week-4',
# '2016-04-week-1', '2016-04-week-2', '2016-04-week-3', '2016-04-week-4',
# '2016-05-week-1', '2016-05-week-2', '2016-05-week-3', '2016-05-week-4',
# '2016-06-week-1', '2016-06-week-2', '2016-06-week-3', '2016-06-week-4',
# '2016-07-week-1', '2016-07-week-2', '2016-07-week-3', '2016-07-week-4',
# '2016-08-week-1', '2016-08-week-2', '2016-08-week-3', '2016-08-week-4',
# '2016-09-week-1', '2016-09-week-2', '2016-09-week-3', '2016-09-week-4',
# '2016-10-week-1', '2016-10-week-2', '2016-10-week-3', '2016-10-week-4',
# '2016-11-week-1', '2016-11-week-2', '2016-11-week-3', '2016-11-week-4',
# '2016-12-week-1', '2016-12-week-2', '2016-12-week-3', '2016-12-week-4',
# '2017-01-week-1', '2017-01-week-2', '2017-01-week-3', '2017-01-week-4',
# '2017-02-week-1', '2017-02-week-2', '2017-02-week-3', '2017-02-week-4',
# '2017-03-week-1', '2017-03-week-2', '2017-03-week-3', '2017-03-week-4',
# '2017-04-week-1', '2017-04-week-2', '2017-04-week-3', '2017-04-week-4',
# '2017-05-week-1', '2017-05-week-2', '2017-05-week-3', '2017-05-week-4',
# '2017-06-week-1', '2017-06-week-2', '2017-06-week-3', '2017-06-week-4',
# '2017-07-week-1', '2017-07-week-2', '2017-07-week-3', '2017-07-week-4',
# '2017-08-week-1', '2017-08-week-2', '2017-08-week-3', '2017-08-week-4',
# '2017-09-week-1', '2017-09-week-2', '2017-09-week-3',
# '2017-10-week-1', '2017-10-week-2', '2017-10-week-3', '2017-10-week-4',
# '2017-11-week-1', '2017-11-week-2', '2017-11-week-3', '2017-11-week-4',
# '2017-12-week-1', '2017-12-week-2', '2017-12-week-3', '2017-12-week-4'] 

tableList = ['2015-01-week-1', '2015-01-week-2', '2015-01-week-3', '2015-01-week-4',
'2015-02-week-1', '2015-02-week-2', '2015-02-week-3', '2015-02-week-4',
'2015-03-week-1', '2015-03-week-2', '2015-03-week-3', '2015-03-week-4',
'2015-04-week-1', '2015-04-week-2', '2015-04-week-3', '2015-04-week-4',
'2015-05-week-1', '2015-05-week-2', '2015-05-week-3', '2015-05-week-4',
'2015-06-week-1', '2015-06-week-2', '2015-06-week-3', '2015-06-week-4',
'2015-07-week-1', '2015-07-week-2', '2015-07-week-3', '2015-07-week-4',
'2015-08-week-1', '2015-08-week-2', '2015-08-week-3', '2015-08-week-4',
'2015-09-week-1', '2015-09-week-2', '2015-09-week-3', '2015-09-week-4', 
'2015-10-week-1', '2015-10-week-2', '2015-10-week-3', '2015-10-week-4',
'2015-11-week-1', '2015-11-week-2', '2015-11-week-3', '2015-11-week-4',
'2015-12-week-1', '2015-12-week-2', '2015-12-week-3', '2015-12-week-4'] 

filename = '2015-All-Lyrics'

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
twitterDataTbl = myclient[dbname]


for idx, table in enumerate(tableList): 
    tweetTbl = twitterDataTbl[table]
    cursor = tweetTbl.find({'score':{'$gt':0.49}})    
    df = pd.DataFrame(list(cursor))
    
    if idx == 0:
        total_df = df
    else:
        frames = [total_df, df]
        total_df = pd.concat(frames)
        
total_df.to_csv(filename + '.csv', index=False)
