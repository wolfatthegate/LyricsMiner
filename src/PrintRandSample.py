import sys
import random
import pymongo
import Helper
import TextCleaner
import nltk

def main():
    cleaner = TextCleaner.TextCleaner()
    
    tablename = sys.argv[1] # '2016-11-Nov-week-1'
    filename = "Rand200-"+ tablename + "-" + sys.argv[2] +".txt"
    
    matchScoreGT = 0.49
    
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    myclient.list_database_names()
    
    twitterDataTbl = myclient['TwitterData']
    twitterDataTbl.list_collection_names()
    
    tweetTbl = twitterDataTbl[tablename]
    myquery = { 'score' : {"$gt": matchScoreGT}}
    
    x = 0
    docs = tweetTbl.find(myquery)
    tweetCount = tweetTbl.find(myquery).count()
    
    sample_size = 200
    if tweetCount < sample_size: 
        sample_size = tweetCount
    
    list_docs = list(docs)
    
    random.shuffle(list_docs)
    randocs = random.sample(list_docs, sample_size)
    counter = 0
    
    with open("exportedData/" + filename, "a") as file:
        for randoc in randocs: 
        
            tweet = cleaner.clean(randoc['tweet'])
            tokenized_str = nltk.word_tokenize(tweet)
    
            if len(tokenized_str)<5: 
                continue
            
            if Helper.findCommonTerms(tweet.lower()): 
                continue
            
            counter += 1 
            file.write('Original - ' + str(randoc['tweet'])+'\n')
            file.write('Clean - ' + tweet+'\n')            
            file.write(str(randoc['suggestions'])+'\n')
            file.write('>>> \n')
            file.write('___________________________\n')
        file.write('There are total {} document which has match score higher than {}\n'.format(tweetCount, matchScoreGT))
        file.write('total {} documents drafted out of {}\n'.format(counter, sample_size))
        
if __name__ == "__main__":
    main()
