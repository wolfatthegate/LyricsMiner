
###  The updates are stored in DB.request as list entries
###  Debugging logs are taken out. 
###  JUN 15, 2021
###  Artist and year fields added after the lyrics match is found. 
###  OCT 31, 2021
###  Author - Waylon Luo

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
import DB
warnings.filterwarnings("ignore")
import time

from gensim.parsing.preprocessing import remove_stopwords
from datetime import datetime
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

def setNewValue(score, suggestions, song, artist, year, type, x_add):
    newvalue = { '$set': {'score': score , \
                          'suggestions': suggestions, \
                          'song': song, \
                          'artist': artist, \
                          'year': year, \
                          'type': type, \
                          'x_add': x_add}}
    return newvalue

def searchTweet(doc):
    try: 
        tweet = doc['tweet']
        obj_id = {'_id': doc['_id']}
        tweetID = doc['tweetID']
        
        # completed scan
        # if doc['type'] == 1: 
        #     return 0
        
        if not tweet: 
            now = datetime.now()
            newvalue = setNewValue(0.00, 'empty tweet', '', '', '', DB.type, now.strftime("%Y-%m-%d %H:%M:%S"))
            DB.tweetTbl.update_one(obj_id, newvalue)
            return 0 
        tweet = cleaner.clean(tweet)
        
        filtered_str = remove_stopwords(tweet)
        filtered_str = Helper.remove_stopWords_custom(filtered_str)
        tokenized_str = nltk.word_tokenize(filtered_str)
        
        '''
        Skip searches for tweets less than 4 words
        '''
        
        if len(tokenized_str) < 5:
            now = datetime.now()
            newvalue = setNewValue(0.01, 'Tweet too short', '', '', '', DB.type, now.strftime("%Y-%m-%d %H:%M:%S"))
            DB.tweetTbl.update_one(obj_id, newvalue)
            return 0 
        
        start = time.time()
        query_list = Helper.findDrugKeywords(tokenized_str)    
        stop = time.time()
       
        if not query_list:
            now = datetime.now()
            newvalue = setNewValue(0.01, 'no drug keywords found', '', '', '', DB.type, now.strftime("%Y-%m-%d %H:%M:%S"))
            DB.tweetTbl.update_one(obj_id, newvalue)
            return 0
        
        for el in tokenized_str: 
            if len(el) > 4: 
                query_list.append(el)
                
        query_list = list(dict.fromkeys(query_list))
        
        go_to_next_tweet = False
        titleMatched = False
        savedline, combined_lines, song, artist, year = '', '', '', '', ''
        suggestions = []
        
        keyword_list_title = []
        keyword_list_lyric = []
        
        for query_word in query_list:
            keyword_list_title.append({"title": {"$regex": query_word, "$options": "-i"}})
            keyword_list_lyric.append({"lyrics1": {"$regex": query_word, "$options": "-i"}})
        
        mytitlequery = {'$and': keyword_list_title}
        mytitle = DB.lyricTbl.find(mytitlequery)
        
        if len(doc['tweet']) > 60:
            temp_str = doc['tweet'][0:60] + ' ... '
        else:
            temp_str = doc['tweet']
        
        # normalizer simplify the words 
        # that spelling checkers cannot handle.
        tweet = normalizer.normalize(tweet)
        # logging.info(mytitlequery, extra = {'_id': doc['_id']})
        # logging.info('searching for ' + temp_str + ' ' + str(mytitle.count()) + \
        #              ' possible titles found.', extra = {'_id': doc['_id']})
        
        for eachtitle in mytitle: 
            title = cleaner.clean(eachtitle['title']) 
            
            result = blaster.SMWalignment(tweet.lower(), title.lower(), DB.threshold)
                
            if round(result[2],2) > 0.70: 
                logging.info('title found', extra = {'_id': doc['_id']})
                titleMatched = True
                suggestions = suggestions + printResult(result, eachtitle['title'], doc['_id'])   
                    
                now = datetime.now()
                newvalue = setNewValue(round(result[2],2), '\n'.join(suggestions), eachtitle['title'], \
                                       eachtitle['name'], eachtitle['year'], \
                                       DB.type, now.strftime("%Y-%m-%d %H:%M:%S"))
                DB.tweetTbl.update_one(obj_id, newvalue)
                break
        
        if titleMatched == True:
            return 0
          
        mylyricquery = {'$and': keyword_list_lyric} # query keyword
        mylyrics = DB.lyricTbl.find(mylyricquery)  #find in lyrics database
    
        # logging.info(mylyricquery, extra= {'_id': doc['_id']})
        # logging.info(str(mylyrics.count()) + ' possible lyrics found.', extra= {'_id': doc['_id']})
        
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
                    result = blaster.SMWalignment(tweet, past_line_1.lower(), DB.threshold)
                    maxScore = result[2]
                    maxMatch = result[3]
                        
                    if result[6] == True:
                        followupLine = past_line_1 + ' ' + past_line_0
                        followUpResult = blaster.SMWalignment(tweet, followupLine.lower(), DB.threshold)
                        maxScore = max(result[2], followUpResult[2], maxScore)
                        maxMatch = max(result[3], followUpResult[3], maxMatch)
                        
                    if result[7] == True:
                        stepBackLine = past_line_2 + ' ' + past_line_1
                        stepBackResult = blaster.SMWalignment(tweet, stepBackLine.lower(), DB.threshold)
                        maxScore = max(result[2], stepBackResult[2], maxScore)
                        maxMatch = max(result[3], stepBackResult[3], maxMatch)
                    
                    if maxScore > DB.high_score:
                        suggestions = suggestions + printResult(result, eachlyrics['title'], doc['_id'])
                        suggestions.append('found the song')
                        song = eachlyrics['title']
                        artist = eachlyrics['name']
                        year = eachlyrics['year']
                        logging.info('found the song', extra= {'_id': doc['_id']})             
                        go_to_next_tweet = True
                        break
                 
            if go_to_next_tweet == True:
                break
            
        suggestions.append('Score: ' + str(round(maxScore,2)) + '/1.0' )
        suggestions = list(dict.fromkeys(suggestions))
        
        now = datetime.now()     
        newvalue = setNewValue(round(maxScore,2), '\n'.join(suggestions), song, artist, year, DB.type, now.strftime("%Y-%m-%d %H:%M:%S"))
        DB.tweetTbl.update_one(obj_id, newvalue)
        stop = time.time()

    except: 
        print('Lyrics Search Error - {}'.format(doc['tweetID']))
