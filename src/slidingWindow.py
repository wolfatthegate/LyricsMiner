import TextCleaner
import pymongo
import blast
import sys
from datetime import datetime
from nltk.tokenize import word_tokenize
import blast
import re

def window_token(text, keyword, window_size):
    
    tokens = word_tokenize(text)
    try: 
        index = tokens.index(keyword)
    except ValueError:
        return []
    
    start = index - window_size
    end = index + window_size
    
    if start < 0: start = 0
    if end > len(tokens)-1: end = len(tokens)
    
    return tokens[start:end]

def tokens_match(tokens, tokens2, threshold):
    
    blaster = blast.blast()
    result = blaster.SMTalignment(tokens, tokens2, threshold)
    if result[2] > 0.67: 
        return True
    else:
        return False  

def main(): 
    
    now = datetime.now()
    table_name = '2016-11-Nov-week-1'
     
    cleaner = TextCleaner.TextCleaner()
    blaster = blast.blast()
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    
    mydb = myclient['TwitterData']
    tweetTbl = mydb[table_name]

    threshold = 0.67
    wsize = 2
    
    keyword = "lit"
        
    query = {'$and': [{'tweet': {"$regex": keyword, "$options": "i"}}, {'excluded': 0}]}
            
    total_doc = tweetTbl.find(query).count()
    print(total_doc)
    
    tweet = ''
    tweet1 = ''
    print_tweet = False
    
    x = 0
    with open('logs/' + table_name + 'slidwindow-search.txt', 'a') as logfile: 
        logfile.write('Total docs for keyword {}: {}\n'.format(keyword, str(total_doc)))

        while x < total_doc: 
            for mydoc in tweetTbl.find(query).skip(x).limit(1): #find() method returns a list of dictionary
                 
                freq = 1
                update_mydoc = {'_id': mydoc['_id']}
                
                tweet = cleaner.clean(mydoc['tweet'].lower())
                tokens = window_token(tweet, keyword.strip(), wsize) 
                
                if print_tweet: 
                    print(tweet)
                
                if len(tokens) == 0: 
                    x += 1
                    continue
                
                for mydoc2 in tweetTbl.find(query).skip(x+1):
                    update_mydoc2 = {'_id': mydoc2['_id']}
                    
                    # compare mydoc and mydoc2
                    
                    tweet1 = cleaner.clean(mydoc2['tweet'])                               
                    tokens2 = window_token(tweet1, keyword.strip(), wsize)
                    
                    if len(tokens2) == 0: 
                        continue
                    
                    flag = tokens_match(tokens, tokens2, threshold)
                    if flag: 
                        logfile.write("MATCH FOUND: {} - {}\n".format(" ".join(tokens), " ".join(tokens2)))
                        logfile.flush()
                        # approximately matched
                        freq = freq + 1
                        # update mydoc2 to excluded
                        tweetTbl.update_one(update_mydoc2, { '$set': {'excluded': 1, 'index': " ".join(tokens)}})
    
                tweetTbl.update_one(update_mydoc, {'$set': {'excluded': 1, 'frequency': freq, 'index': keyword}})
            
            x = x + 1 
    
        now = datetime.now()
        dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    
        logfile.write('Keyword {} search finished at {}\n'.format(keyword, dt_string))    
        
if __name__ == "__main__":
    main()