'''

This program is to process Jianfeng's 
tweets with three keys in Json objects. 
Jun 28, 2021

'''
import TextCleaner
import nltk
import Helper
import glob
import pymongo
import json
from datetime import datetime
from gensim.parsing.preprocessing import remove_stopwords

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient['TwitterData']
folderlist = ['2019-03-week-1']
cleaner = TextCleaner.TextCleaner()

def ImportTweets(filepath):
    with open(filepath, 'r') as f:
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
                follower = jsonObj['followers']
                                
                filtered_str = remove_stopwords(tweet)
                tokenized_str = nltk.word_tokenize(filtered_str)
                
                if Helper.findCommonTerms(tokenized_str, cleaner.clean(tweet)) == True:
                    print('common Term: ', tweet)
                    continue
                
                # exclude short tweets
                if len(tokenized_str) < 4:
                    continue
                
                if Helper.findDrugKeywordsForRawTweets(tokenized_str) == False: 
                    continue 
                
                data = {'tweetID': tweetID,
                        'tweet' : tweet, 
                        'createdAt': createdAt,
                        'userid': userid, 
                        'follower': follower}
                
                try:
                    myTbl.insert_one(data)
                except:
                    print('insertion error. TweetID: ' + tweetID)
            except Exception as e:
                print('json object error: ', e)
            
                #logfile.write('duplicate data. TweetID: {}. Tweet {}. Filepath {}\n'.format(tweetID, description, filepath)) 
    
        logfile.write('finished importing file: {} at {}\n'.format(filepath, datetime.now().time())) 



for folder in folderlist: 
    myTbl = mydb[folder]
    filepathList = glob.glob(folder + "/*.json")
    
    for filepath in filepathList: 
        with open('logs/' + folder + '_json_log.txt', 'a') as logfile:
            ImportTweets(filepath)
    print('Finished Importing: ' + folder + '\n')    




