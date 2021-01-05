import nltk
import blast
import re
from gensim.parsing.preprocessing import remove_stopwords

def findDrugKeywords(str):
    
    terms = ['heroin', 'heroine', 'oxy', 'dopamine', 'norepinephrine',
             'weed','cocaine', 'lean', 'blunt', 'joint', 'dank',
             'crack', 'molly', 'coke', 'smoke', 'dope', 'cigarette',
             'smoking', 'smokin', 'pour', 'xan', 'crack in my crack']
    
    substract_terms = ['u', 'ex', 'help', 'th',
                       'dont', 'gon', 'na', 'hos', 'like', 
                       'before', 'im ', 'nigga', 'bieber', 'hand', 
                       'albany', 'people', 'diamonds', 'glow', 'aint', 
                       'run', 'bout', 'fun', 'comin']
    
    ### initialization
    filtered_str = remove_stopwords(str)
    tokenized_str = nltk.word_tokenize(filtered_str)
    blaster = blast.blast()
    keywordList = []
    
    for tokenized_word in tokenized_str:
        for term in terms:      
            result = blaster.SMalignmentGlobal(tokenized_word.lower(), term.lower())
            if result[2] > 0.85:
                term = re.sub(r'ing$', 'in', term)
                keywordList.append(term.lower())   
    if keywordList:                       
        for word_token in tokenized_str: 
            if word_token.lower() not in substract_terms:  
#                 word_token = spell.correction(word_token)          
                word_token = re.sub(r'ing$', 'in', word_token)            
                keywordList.append(word_token.lower()) 
    keywordList = list(dict.fromkeys(keywordList))
#     print(keywordList)
    return keywordList

def findArtistName(str):
    artistList = ['young thug', 'coke boys']

    for artist in artistList:
        str = str.lower()               
        if (str.find(artist)!=-1):
            return artist
    return ''