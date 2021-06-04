import pymongo
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
twitterDataTbl = myclient['TwitterData']
tweetTbl = twitterDataTbl['2017-10-week-2']