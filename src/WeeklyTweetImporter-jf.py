'''
This program is to process Jianfeng's 
tweets without any filters. 
July 18, 2021

'''
import TextCleaner
import nltk
import Helper
import glob
import pymongo
import json
import os
from datetime import datetime
from gensim.parsing.preprocessing import remove_stopwords

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient['TwitterData']
folderlist = ['2017-10-w1-JF', '2017-10-w2-JF', '2017-10-w3-JF', '2017-10-w4-JF']
cleaner = TextCleaner.TextCleaner()

def ImportTweets(filepath):
    with open(filepath, 'r') as f:
        logfile.write('started importing file: {} at {}\n'.format(filepath, datetime.now().time()))
        for line in f:
            line = line.strip() # read only the first jsonObj line
            try: 
                jsonObj = json.loads(line) # load it as Python dictionary
    #         print(json.dumps(jsonObj, indent = 4)) # pretty-print
            except Exception as e:
                print('json object error: ', e)
                print(line)
                print(filepath)
                
            try:
                tweetID = str(jsonObj['id'])               
                tweet = jsonObj['text'] # actual jsonObj
                createdAt = jsonObj['created_at']
                userid = jsonObj['user_id']
                follower = jsonObj['followers_count']
                
                                # exclude short tweets
                if len(tweet.split()) < 4:
                    data = {'tweetID': tweetID,
                        'tweet' : tweet, 
                        'createdAt': createdAt,
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
                        'userid': userid, 
                        'follower': follower, 
                        'suggestions': 'common term', 
                        'type':1}
                    try:
                        myTbl.insert_one(data)
                    except:
                        print('insertion error. TweetID: ' + tweetID)
                    continue
                
                data = {'tweetID': tweetID,
                        'tweet' : tweet, 
                        'createdAt': createdAt,
                        'userid': userid, 
                        'follower': follower,
                        'suggestions': '',
                        'type':0}
                
                try:
                    myTbl.insert_one(data)
                except:
                    print('insertion error. TweetID: ' + tweetID)
            except Exception as e:
                print('json object error: ', e)
            
                #logfile.write('duplicate data. TweetID: {}. Tweet {}. Filepath {}\n'.format(tweetID, description, filepath)) 
    
        os.remove(filepath)


for folder in folderlist: 
    myTbl = mydb[folder]
    filepathList = glob.glob(folder + "/*.json")
    
    for filepath in filepathList: 
        with open('logs/' + folder + '_json_import_log.txt', 'a') as logfile:
            ImportTweets(filepath)
    print('Finished Importing: ' + folder + '\n')    




