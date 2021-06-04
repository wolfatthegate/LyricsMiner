'''
This program is for MongoDB tables which doesn't have 
tweet IDs. The program finds tweet IDs from file and 
add them to the existing entries. 
'''

import glob
import pymongo
import json
from datetime import datetime

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient['TwitterData']

def FindAndImportID(filepath, tablename):
    tweetTbl = mydb[tablename]
    with open(filepath, 'r') as f:
        for line in f:
            line = line.strip() # read only the first jsonObj line
            jsonObj = json.loads(line) # load it as Python dictionary
    #         print(json.dumps(jsonObj, indent = 4)) # pretty-print
            
            try:
                tweetID = str(jsonObj['id'])
                retweeted = 0 if jsonObj['retweeted'] is False else 1        
                userid = jsonObj['user']['id']
                follower = jsonObj['user']['followers_count']    
                               
                if tweetTbl.find({'tweetID': tweetID}).count() is 1: 

                    newvalue = { '$set': {'retweeted': retweeted, 
                                            'userid': userid,
                                            'follower': follower}}
                    
                    mydoc = tweetTbl.find({'tweetID': tweetID})
                    updatequery = {'_id': mydoc[0]['_id']}
                    tweetTbl.update_one(updatequery, newvalue)
            except:
                print('error importing')
    
        logfile.write('finished inserting IDs for file: {} at {}\n'.format(filepath, datetime.now().time())) 

folderlist = ['2016-10-week-1']

for folder in folderlist: 
    myTbl = mydb[folder]
    filepathList = glob.glob(folder + "/*.json")
    
    for filepath in filepathList: 
        with open('logs/' + folder + '_json_log.txt', 'a') as logfile:
            FindAndImportID(filepath, folder)
    print('Finished Importing IDs: ' + folder + '\n')    




