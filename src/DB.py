'''
Author - Waylon Luo
'''

import pymongo
import logging

tablenames = [['2016-01-week-2', '2016-01-week-3', '2016-01-week-4'],
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
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient['LyricsDB']
lyricTbl = mydb['Lyrics']
twitterDataTbl = myclient['TwitterData']
tweetTbl = twitterDataTbl['2016-01-week-2']

logFormatter = '%(asctime)s - %(_id)s - %(levelname)s - %(message)s'
logging.basicConfig(filename='logs/info.log',level=logging.INFO, format='%(asctime)s %(_id)s %(levelname)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

threshold = 0.65
high_score = 0.49
type = 1
