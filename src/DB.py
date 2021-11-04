'''
Author - Waylon Luo
'''

import pymongo
import logging

tablenames = ['2015-08-week-1', '2015-08-week-2', '2015-08-week-3', '2015-08-week-4']
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
