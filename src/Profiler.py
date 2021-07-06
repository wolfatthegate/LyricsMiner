import pymongo
import sys

def main(): 

    ### DB connection

    table_name = '2016-10-week-1'
    table_name_1 = '2016-10-week-1-1'
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    twitterDB = myclient['TwitterData']
    tweetTbl = twitterDB[table_name]
    tweetTbl_1 = twitterDB[table_name_1]
    
    ### Query
    
    dataList = tweetTbl.find({'score': {'$gt': 0.49}})
    
    ### initialization
    
    user_dict = {}

    for data in dataList: 
        userdataList = tweetTbl_1.find({'userid': data['userid']})
        
        user_id =  data['userid']
        if user_id in user_dict.keys():
            continue
        tweets = []
        for userdata in userdataList: 
            tweets.append(userdata['tweet'].replace('\n', ' '))
        user_dict[data['userid']] = tweets
    
    with open('logs/'+table_name+'profiles.log', 'a') as profile: 
        for key, value in user_dict.items(): 
            profile.write( '{}\t{}\n'.format(key, '\n\t'.join(value)))    
    
if __name__ == "__main__":
    main()

