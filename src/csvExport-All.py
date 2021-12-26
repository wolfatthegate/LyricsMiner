import sys
import pymongo
import pandas as pd

dbname = 'TwitterData' #TwitterData #tweet_drugs
tableList = ['2016-01-week-2', '2016-01-week-3', '2016-01-week-4',
'2016-02-week-1', '2016-02-week-2', '2016-02-week-3',
'2016-03-week-2', '2016-03-week-3', '2016-03-week-4',
'2016-04-week-1', '2016-04-week-2', '2016-04-week-3', '2016-04-week-4',
'2016-05-week-1', '2016-05-week-2', '2016-05-week-3', '2016-05-week-4',
'2016-06-week-1', '2016-06-week-2', '2016-06-week-3', '2016-06-week-4',
'2016-07-week-1', '2016-07-week-2', '2016-07-week-3', '2016-07-week-4',
'2016-08-week-1', '2016-08-week-2', '2016-08-week-3', '2016-08-week-4',
'2016-09-week-1', '2016-09-week-2', '2016-09-week-3', '2016-09-week-4',
'2016-10-week-1', '2016-10-week-2', '2016-10-week-3', '2016-10-week-4',
'2016-11-week-1', '2016-11-week-2', '2016-11-week-3', '2016-11-week-4',
'2016-12-week-1', '2016-12-week-2', '2016-12-week-3', '2016-12-week-4',
'2017-01-week-1', '2017-01-week-2', '2017-01-week-3', '2017-01-week-4',
'2017-02-week-1', '2017-02-week-2', '2017-02-week-3', '2017-02-week-4',
'2017-03-week-1', '2017-03-week-2', '2017-03-week-3', '2017-03-week-4',
'2017-04-week-1', '2017-04-week-2', '2017-04-week-3', '2017-04-week-4',
'2017-05-week-1', '2017-05-week-2', '2017-05-week-3', '2017-05-week-4',
'2017-06-week-1', '2017-06-week-2', '2017-06-week-3', '2017-06-week-4',
'2017-07-week-1', '2017-07-week-2', '2017-07-week-3', '2017-07-week-4',
'2017-08-week-1', '2017-08-week-2', '2017-08-week-3', '2017-08-week-4',
'2017-09-week-1', '2017-09-week-2', '2017-09-week-3',
'2017-10-week-1', '2017-10-week-2', '2017-10-week-3', '2017-10-week-4',
'2017-11-week-1', '2017-11-week-2', '2017-11-week-3', '2017-11-week-4',
'2017-12-week-1', '2017-12-week-2', '2017-12-week-3', '2017-12-week-4'] 
filename = '-All-Lyrics'

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
twitterDataTbl = myclient[dbname]

for table in tableList: 
    tweetTbl = twitterDataTbl[table]
    cursor = tweetTbl.find({'score':{'$gt':0.49}})    
    df = pd.DataFrame(list(cursor))
    df.to_csv(table + filename + '.csv', index=False)
