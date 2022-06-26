import pymongo
import logging
import time
import TextCleaner
import pymongo
import nltk
import blast
import sys
import logging
import Normalizer
import re
import warnings
import Helper

from pymongo import UpdateOne
from datetime import datetime
from gensim.parsing.preprocessing import remove_stopwords

warnings.filterwarnings("ignore")

tablename = 'tweets_valid_not_us'
lyricsMatchTbl = 'lyrics_match_valid_not_us'

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient['LyricsDB']
lyricTbl = mydb['Lyrics']
twitterDataTbl = myclient['tweet_drugs']
tweetTbl = twitterDataTbl[tablename]
lyricsMatchTbl = twitterDataTbl[lyricsMatchTbl]

scanned_valid = twitterDataTbl['scanned_valid_not_us']

logFormatter = '%(asctime)s - %(_id)s - %(levelname)s - %(message)s'
logging.basicConfig(filename='logs/'+tablename+'_Logs.log',level=logging.INFO, format='%(asctime)s %(_id)s %(levelname)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

threshold = 0.65
high_score = 0.49

# from spellchecker import SpellChecker

cleaner = TextCleaner.TextCleaner()
normalizer = Normalizer.Normalizer()
blaster = blast.blast()

def printResult(result, songtitle, doc_id):
    
    suggestions = []
    suggestions.append('tweet: ' + result[0])
    suggestions.append('query: ' + result[1])
    suggestions.append('(' + str(result[3]) + ' words matched)')
    
    return suggestions

def setNewValue(score, suggestions, song, artist, x_add):
    newvalue = {'score': score, 'suggestions': suggestions, \
                'song': song, 'artist': artist, 'x_add': x_add}
    return newvalue

def searchTweet(doc):
    try: 
        tweet = doc['text']
        tweetID = doc['id_str']
        
        if not tweet: 
            ''' empty tweet '''
            return 0 
        
        tweet = cleaner.clean(tweet)      
        filtered_str = remove_stopwords(tweet)
        filtered_str = Helper.remove_stopWords_custom(filtered_str)
        tokenized_str = nltk.word_tokenize(filtered_str)
        
        if len(tokenized_str) < 5:
            '''Tweet too short'''
            return 0 
        
        start = time.time()
        query_list = Helper.findDrugKeywords(tokenized_str)    
        stop = time.time()
       
        if not query_list:
            '''no drug keywords found'''
            return 0
        
        for el in tokenized_str: 
            if len(el) > 4: 
                query_list.append(el)
                
        query_list = list(dict.fromkeys(query_list))
        
        go_to_next_tweet = False
        titleMatched = False
        savedline, combined_lines, song = '', '', ''
        suggestions = []
        
        keyword_list_title = []
        keyword_list_lyric = []
        
        for query_word in query_list:
            keyword_list_title.append({"title": {"$regex": query_word, "$options": "-i"}})
            keyword_list_lyric.append({"lyrics1": {"$regex": query_word, "$options": "-i"}})
        
        mytitlequery = {'$and': keyword_list_title}
        mytitle = lyricTbl.find(mytitlequery)
        
        if len(doc['text']) > 60:
            temp_str = doc['text'][0:60] + ' ... '
        else:
            temp_str = doc['text']
        
        # normalizer simplify the words that spelling checkers cannot handle.
        
        tweet = normalizer.normalize(tweet)
        
        for eachtitle in mytitle: 
            title = cleaner.clean(eachtitle['title']) 
            
            result = blaster.SMWalignment(tweet.lower(), title.lower(), threshold)
                
            if round(result[2],2) > 0.70: 
                logging.info('title found', extra = {'id_str': doc['id_str']})
                titleMatched = True
                suggestions = suggestions + printResult(result, eachtitle['title'], doc['id_str'])   
                    
                now = datetime.now()
                lyricsMatchTbl.insert_one(doc)
                newvalue = setNewValue(round(result[2],2), '\n'.join(suggestions), eachtitle['title'], \
                                       eachtitle['name'], now.strftime("%Y-%m-%d %H:%M:%S"))
                lyricsMatchTbl.update_one({'_id': doc['_id']}, {"$set": newvalue})

                break
        
        if titleMatched == True:
            return 0
          
        mylyricquery = {'$and': keyword_list_lyric} # query keyword
        mylyrics = lyricTbl.find(mylyricquery)  #find in lyrics database
        maxScore = 0.0
        start = time.time()
        
        for eachlyrics in mylyrics: #loop through mylyrics list
            
            # initialize variables 
            continue_test = False  
    
            # read each line of lyrics  
            past_line_2 = ''
            past_line_1 = ''
            past_line_0 = ''
            combined_string = ''
            
            maxScore = 0.0 #reset
            maxMatch = 0
      
            for eachline in eachlyrics['lyrics1'].splitlines():
                
                eachline = cleaner.clean(eachline)
                
                past_line_2 = past_line_1
                past_line_1 = past_line_0
                past_line_0 = eachline
                
                combined_string = past_line_2 + ' ' + past_line_1 + ' ' + past_line_0
                for query_word in query_list:
                    if query_word.lower() in combined_string.lower():
                        continue_test = True
                            
                if continue_test == True:
    
                    # perform blast Search                    
                    result = blaster.SMWalignment(tweet, past_line_1.lower(), threshold)
                    maxScore = result[2]
                    maxMatch = result[3]
                        
                    if result[6] == True:
                        followupLine = past_line_1 + ' ' + past_line_0
                        followUpResult = blaster.SMWalignment(tweet, followupLine.lower(), threshold)
                        maxScore = max(result[2], followUpResult[2], maxScore)
                        maxMatch = max(result[3], followUpResult[3], maxMatch)
                        
                    if result[7] == True:
                        stepBackLine = past_line_2 + ' ' + past_line_1
                        stepBackResult = blaster.SMWalignment(tweet, stepBackLine.lower(), threshold)
                        maxScore = max(result[2], stepBackResult[2], maxScore)
                        maxMatch = max(result[3], stepBackResult[3], maxMatch)
                    
                    if maxScore > high_score:
                        suggestions = suggestions + printResult(result, eachlyrics['title'], doc['id_str'])
                        suggestions.append('found the song')
                        song = eachlyrics['title']
                        artist = eachlyrics['name']
                        logging.info('found the song', extra= {'id_str': doc['id_str']})             
                        go_to_next_tweet = True
                        break
                 
            if go_to_next_tweet == True:
                suggestions.append('Score: ' + str(round(maxScore,2)) + '/1.0' )
                suggestions = list(dict.fromkeys(suggestions))
                
                now = datetime.now()     
                lyricsMatchTbl.insert_one(doc)
                newvalue = setNewValue(round(result[2],2), '\n'.join(suggestions), song, artist, now.strftime("%Y-%m-%d %H:%M:%S"))
                lyricsMatchTbl.update_one({'_id': doc['_id']}, {"$set": newvalue})
                stop = time.time()
                break
        
        return 0 

    except: 
        print('Lyrics Search Error - Tweet ID: {}'.format(doc['id_str']))

def main():
    
    ### Initialization
    ### get the number of document which are already scanned.    
    myquery = {}
    count = 0
     
    rowid = scanned_valid.find().count() 
    cursor = tweetTbl.find(myquery).skip(rowid) #find() method returns a list of dictionary
    sessionstart = time.time()
    
    batchsize = 5000
    while cursor.alive: 
        try: 
            doc = cursor.next()
            searchTweet(doc)
            count+=1
            scanned_valid.insert_one({ 'rowid': rowid, 'tweet_id': doc['id_str']})
            rowid+=1
            
            if count%(batchsize) == 0 :  ###   Write a log after every x document
                sessionstop = time.time()
                totaltime = sessionstop - sessionstart
                with open('logs/'+ tablename +'Log.log', 'a') as logfile:
                    now = datetime.now()
                    dt_string = now.strftime("%m/%d/%Y %H:%M:%S")
                    logfile.write('{}: {} scanned {} documents from table {}\n'.format(tablename, dt_string, count, tablename)) 
                    print('{}: {} scanned {} documents from table {}\n'.format(tablename, dt_string, count, tablename))
                    print('{} tweets per second'.format(round(batchsize/totaltime,4)))
                sessionstart = time.time()
        except StopIteration:
            print('error in mainv2')
            time.sleep(1)
            
    print('total number of docs inserted - {}'.format(count))
    print('done..')

if __name__ == "__main__":
    print('start main - scanning {}'.format(tablename))
    start = time.time()
    main()
    stop = time.time()
    print("total time: {}".format(stop-start))
