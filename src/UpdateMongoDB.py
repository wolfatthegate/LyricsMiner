import pymongo
import sys
 
myclient = pymongo.MongoClient("mongodb://localhost:27017/")

mydb = myclient['TwitterData']
lyricTbl = mydb['2016-10-week-1']

myquery = {"suggestions": "filtered"}
noofdoc = lyricTbl.find(myquery).count()

x = 0
inc = 5
while x < noofdoc:
    for mydoc in lyricTbl.find(myquery).skip(x).limit(inc): #find() method returns a list of dictionary
        updatequery = {'_id': mydoc['_id']}
        newvalue = { '$set': {'score': 0.51}}
        lyricTbl.update_one(updatequery, newvalue)
    x = x + inc 



            
                