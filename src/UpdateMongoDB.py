import pymongo
import sys
 
myclient = pymongo.MongoClient("mongodb://localhost:27017/")

mydb = myclient['TwitterData']
lyricTbl = mydb['2017-07-16-WeeklyTweets']

myquery = {'score': {'$and': [{'$lt': 0.50}, {'$gt': 0.00}]}}
noofdoc = lyricTbl.find(myquery).count()

x = 0
inc = 5
while x < noofdoc:
    for mydoc in lyricTbl.find(myquery).skip(x).limit(inc): #find() method returns a list of dictionary
        updatequery = {'_id': mydoc['_id']}
        newvalue = { '$set': {'type': 0}}
        lyricTbl.update_one(updatequery, newvalue)
    x = x + inc 



            
                