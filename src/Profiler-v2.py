import pymongo
import sys

def main(): 

    ### DB connection

    table_name = '2016-10-week-3'
    table_name_1 = '2016-10-week-3-1'
    
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    twitterDB = myclient['TwitterData']
    tweetTbl = twitterDB[table_name]
    tweetTbl_1 = twitterDB[table_name_1]
    
    ### Query
    
    dataList = tweetTbl.find({'score': {'$gt': 0.49}})
    
    ### initialization
    
    user_dict = {}

    for data in dataList: 
        tweet = tweetTbl_1.find({'tweetID': data['tweetID']})
        
        if tweet.count() > 0:
            if 'userid' in tweet[0]:
                user_id =  tweet[0]['userid']
            else:
                continue
            if 'follower' in tweet[0]:
                follower = tweet[0]['follower']
            else: 
                follower = 0
        else:
            continue
        if user_id in user_dict.keys():
            continue
        
        tweets = []
        userdataList = tweetTbl_1.find({'userid': user_id})
        
        for userdata in userdataList: 
            tweets.append(str(follower) + '\t' + userdata['tweet'].replace('\n', ' '))
        user_dict[user_id] = tweets
    
    with open('logs/'+table_name+'profiles.log', 'a') as profile: 
        profile.write('{}\t{}\t{}\n'.format('user id', 'follower', 'tweet'))
        for key, value in user_dict.items(): 
            profile.write( '{}\t{}\n'.format(key, '\n\t'.join(value)))    
    
if __name__ == "__main__":
    main()

