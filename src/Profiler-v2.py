### Profiler list the Lyrics tweeters for JuanZhi's Data
### Author - Waylon Luo

import pymongo
import sys
import pandas as pd
import numpy as np

def main(): 

    ### DB connection

    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    db = myclient['tweet_drugs']
    tbl = db['lyrics_match']
    tvtbl = db['tweets_valid']
    matchList = tbl.find({'score': {'$gt': 0.49}})

    filename = 'users_followup'
    
    ### tweetIDs of Lyrics matches. 
    tweetIDs = []
    userIDs = []
    for match in matchList:
        tweetIDs.append(match['id_str'])

    
    for tweetID in tweetIDs: 
        tweetObj = tvtbl.find({'_id': tweetID})
        userIDs.append(tweetObj['_user_id_str'])
        
    ### filter unique user ID list    
    userIDs = list(set(userIDs))
    
    ### csv table for data
    csvtbl = [] 
    
    for userID in userIDs: 
        
        ### Query
        tweetObjs = tvtbl.find({'_user_id_str': userID})
        
        data = []
        ### Iterate all the tweets from same user
        for tweetObj in tweetObjs: 
            
            data.append(tweetObj['_user_id_str'])
            data.append(tweetObj['_id'])
            data.append(tweetObj['_full_text'])
            data.append(tweetObj['quote_count'])
            data.append(tweetObj['reply_count'])
            data.append(tweetObj['retweet_count'])
            data.append(tweetObj['favorite_count'])
        
        csvtbl.append(data)

    df =  pd.DataFrame(np.array(csvtbl), 
                       columns = ['_user_id_str', '_id', '_full_text', \
                                  'quote_count', 'reply_count', \
                                  'retweet_count', 'favorite_count'])
    
    df.to_csv(filename+'.csv', index=False)
    
if __name__ == "__main__":
    main()
