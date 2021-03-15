''' 
Group the similar tweets and report the frequency of each tweets. 
'''

import TextCleaner
import pymongo
import blast
import sys
import nltk
from datetime import datetime

def main(): 
    filename = sys.argv[1]
    now = datetime.now()
    table_name = '2016-11-Nov-week-1'
     
    cleaner = TextCleaner.TextCleaner()
    blaster = blast.blast()
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    
    mydb = myclient['TwitterData']
    tweetTbl = mydb[table_name]
    
    search_words = []
    with open(filename, 'r') as dList: 
        for d in dList: 
            search_words.append(d.strip())
        search_words.append('')
    
    with open('logs/' + table_name + 'freq-search.txt', 'a') as logfile: 
        dt_string = now.strftime("%d-%m-%Y_%H-%M-%S")
        logfile.write("start date and time =" + dt_string + '\n')   
        
        for search_word in search_words: 
            query = {'$and': [{'tweet': {"$regex": search_word}}, {'excluded': 0}]}
            
            total_doc = tweetTbl.find(query).count()
      
            tweet = ''
            tweet1 = ''
            
            x = 0
            
            logfile.write('total doc for '+ search_word + ': ' + str(total_doc) +'\n')
            while x < total_doc: 
                for mydoc in tweetTbl.find(query).limit(1): #find() method returns a list of dictionary
                     
                    freq = 1
                    update_mydoc = {'_id': mydoc['_id']}
                    
                    for mydoc2 in tweetTbl.find(query).skip(1):
                        update_mydoc2 = {'_id': mydoc2['_id']}
                        
                        # compare mydoc and mydoc2
                        tweet = cleaner.clean(mydoc['tweet'])
                        tweet1 = cleaner.clean(mydoc2['tweet'])
                        
                        tokenized_tweet = nltk.word_tokenize(tweet)
                        tokenized_tweet1 = nltk.word_tokenize(tweet1)
                        
                        if abs(len(tokenized_tweet) - len(tokenized_tweet1)) > 4:                
                            continue
                        
                        result = blaster.SMWalignment(tweet, tweet1, 0.75)
                        
                        if result[2] > 0.6:
                            # approximately matched
                            freq = freq + 1
                            # update mydoc2 to excluded
                            tweetTbl.update_one(update_mydoc2, { '$set': {'excluded': 1, 'index': search_word}})
                        
                    tweetTbl.update_one(update_mydoc, {'$set': {'excluded': 1, 'frequency': freq, 'index': search_word}})
                if freq > 5: 
                    logfile.write('total freq for \"'+ tweet + '\": ' + str(freq) +'\n')
                x = x + 1 
        
            now = datetime.now()
            dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    
        print(search_word + ' finished')
    
if __name__ == "__main__":
    main()