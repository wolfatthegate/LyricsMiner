import sys
import random
import pymongo

def main():
    
    tablename = sys.argv[1] # '2016-11-Nov-week-1'
    filename = "Rand1000-"+ tablename + "-" + sys.argv[2] +".txt"
    
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    myclient.list_database_names()
    
    twitterDataTbl = myclient['TwitterData']
    twitterDataTbl.list_collection_names()
    
    tweetTbl = twitterDataTbl[tablename]
    myquery = {}
    
    x = 0
    docs = tweetTbl.find(myquery)
    tweetCount = tweetTbl.find(myquery).count()
    
    sample_size = int(sys.argv[2])
    if tweetCount < sample_size: 
        sample_size = tweetCount
    
    list_docs = list(docs)
    
    random.shuffle(list_docs)
    randocs = random.sample(list_docs, sample_size)
    counter = 0
    
    with open("exportedData/" + filename, "a") as file:
        for randoc in randocs: 
        
            tweet = randoc['tweet'].replace('\n', '')
            file.write(tweet+'\n')
            
if __name__ == "__main__":
    main()
