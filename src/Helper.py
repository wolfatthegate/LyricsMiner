import nltk
import re
import BST
import json
import CommonTermsDict as ctd
import blast
from gensim.parsing.preprocessing import remove_stopwords

dkTree = BST.Node(None) #drug keywords in BST
with open("keywords/DrugListShort.txt", "r") as file:
    for el in file: 
        dkTree.insert(str(el).strip())


stTree = BST.Node(None) #substracted terms in BST
with open("keywords/SubstractTerms.txt", "r") as file: 
    for el in file: 
        stTree.insert(str(el).strip())
        
ctTree = BST.Node(None) #common terms in BST
with open("keywords/CommonTerms.txt", "r") as file: 
    for el in file: 
        ctTree.insert(str(el).strip())

def findCommonTerms(str):
    
    filtered_str = remove_stopwords(str)
    tokenized_str = nltk.word_tokenize(filtered_str)
    blaster = blast.blast()
    
    for token in tokenized_str: 
        if token in ctd.ctDict:
            for el in ctd.ctDict[token]: 
                # compare el with str )
                result = blaster.SMWalignment(el, str, threshold = 0.80)
                if result[2] > 0.65: 
                    return True           
    return False

def findDrugKeywords(str):
    
    ### initialization
    filtered_str = remove_stopwords(str)
    tokenized_str = nltk.word_tokenize(filtered_str)
    keywordList = []
    found = False
    for tokenized_word in tokenized_str:
        found = dkTree.findval(tokenized_word)
        if found == True: 
            tokenized_word = re.sub(r'ing$', 'in', tokenized_word)
            keywordList.append(tokenized_word.lower())  
             
    if keywordList:                       
        for word_token in tokenized_str: 
            found = stTree.findval(word_token.lower())
            if found == False:  
                word_token = re.sub(r'ing$', 'in', word_token)            
                keywordList.append(word_token.lower()) 

    keywordList = list(dict.fromkeys(keywordList))

    return keywordList

def findArtistName(str):
    artistList = ['young thug', 'coke boys']

    for artist in artistList:
        str = str.lower()               
        if (str.find(artist)!=-1):
            return artist
    return ''