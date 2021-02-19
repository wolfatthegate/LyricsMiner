import glob
filepathList = glob.glob("2016-11-week-3/*.json")

import pymongo
import json
from datetime import datetime

print('import finished in the past')
print('quitting')
quit()
#print('importing..')

myclient = pymongo.MongoClient("mongodb://localhost:27017/")

mydb = myclient['TwitterData']
myTbl = mydb['2016-11-Nov-week-3']

def TweetImporter(filepath):
    with open(filepath, 'r') as f:
        
        data = json.load(f) 
        filepath = filepath[15:].replace('.json', '')
        filepath = filepath.replace('twitter_', '')

        for el in data:

            data = {'filename': filepath,
                    'tweetID': el['tweet_id'],
                    'tweet': el['text'], 
                    'userID': el['user_id']
            }
            
            try: 
                myTbl.insert_one(data)    
            except: 
                print('insertion error. TweetID: ' + el['tweet_id'])
            
        logfile.write('finished importing file: {} at {}\n'.format(filepath, datetime.now().time())) 

for filepath in filepathList: 
    with open('logs/2016_json_log.txt', 'a') as logfile:
        TweetImporter(filepath)
    
print('Finished')
