'''
Twitter files that I have been receiving from Han Hu
have two different versions. 
1) partial fields/data
2) all fields/full data

This program can handle the files with full data. 
April 10, 2021

UserID and Follower Count is added. 
May 25, 2021
'''
import TextCleaner
import glob
import pymongo
import json
import nltk
from gensim.parsing.preprocessing import remove_stopwords
import Helper
from datetime import datetime
import os

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient['TwitterData']
cleaner = TextCleaner.TextCleaner()

def ImportTweets(filepath):
    with open(filepath, 'r') as f:
        logfile.write('started importing file: {} at {}\n'.format(filepath, datetime.now().time())) 
        for line in f:
            line = line.strip() # read only the first jsonObj line
            jsonObj = json.loads(line) # load it as Python dictionary
    #         print(json.dumps(jsonObj, indent = 4)) # pretty-print
            
            try:
                if jsonObj['lang'] != 'en':
                    continue  
                tweetID = str(jsonObj['id'])               
                tweet = jsonObj['text'] # actual jsonObj
                
                
                createdAt = jsonObj['created_at']
                retweeted = 0 if jsonObj['retweeted'] is False else 1        
                userid = jsonObj['user']['id']
                
                try: 
                    description = jsonObj['user']['description']
                except: 
                    description = ''
                follower = jsonObj['user']['followers_count']   
                             
                try:
                    location = jsonObj['place']['full_name']
                except: 
                    location = ''
                
                if len(tweet.split()) < 4:
                    data = {'tweetID': tweetID,
                        'tweet' : tweet, 
                        'createdAt': createdAt,
                        'retweeted': retweeted,
                        'userid': userid, 
                        'follower': follower, 
                        'suggestions': 'tweet too short', 
                        'type':1}
                    try:
                        myTbl.insert_one(data)
                    except:
                        print('insertion error. TweetID: ' + tweetID)
                    continue
                                    
                filtered_str = remove_stopwords(tweet)
                tokenized_str = nltk.word_tokenize(filtered_str)
                
                if Helper.findCommonTerms(tokenized_str, cleaner.clean(tweet)) == True:
                    data = {'tweetID': tweetID,
                        'tweet' : tweet, 
                        'createdAt': createdAt,
                        'retweeted': retweeted,
                        'userid': userid, 
                        'follower': follower, 
                        'suggestions': 'common term', 
                        'type':1}
                    try:
                        myTbl.insert_one(data)
                    except:
                        print('insertion error. TweetID: ' + tweetID)
                    continue
                
                # exclude short tweets
                    
                data = {'tweetID': tweetID,
                        'tweet' : tweet, 
                        'createdAt': createdAt,
                        'retweeted': retweeted, 
                        'userid': userid,
                        'follower': follower,
                        'suggestions': '',
                        'type':0}
                try:
                    myTbl.insert_one(data)
                except:
                    print('insertion error. TweetID: ' + tweetID)
            except:
                print('json object error')
                #logfile.write('duplicate data. TweetID: {}. Tweet {}. Filepath {}\n'.format(tweetID, description, filepath)) 
        os.remove(filepath)
        

folderlist = ['2016-12-week-3']

for folder in folderlist:
     
    myTbl = mydb[folder]
    filepathList = glob.glob(folder + "/*.json")
    
    for filepath in filepathList: 
        with open('logs/' + folder + '_json_log.txt', 'a') as logfile:
            ImportTweets(filepath)
    print('Finished Importing: ' + folder + '\n')    




