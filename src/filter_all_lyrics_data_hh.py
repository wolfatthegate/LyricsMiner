import json
import os, sys
import tarfile, gzip
import codecs
import re
import logging
import numpy as np

from time import time
from datetime import timedelta, date
from multiprocessing import Pool
from multiprocessing import Process, Manager
import traceback
import numpy as np
import pandas as pd

import preprocessor

from nltk.stem.wordnet import WordNetLemmatizer
from nltk.corpus import wordnet
from nltk import pos_tag

from fuzzywuzzy import fuzz

import multiprocessing_logging

# from filelock import Timeout, FileLock

basepath = '2016-12/'
output_basepath = 'drug_tweets_stage3/2016-12/'
year = 2016

# setup the logger
logging.basicConfig(level=logging.INFO,
										format='%(asctime)s: %(levelname)s: %(message)s')
multiprocessing_logging.install_mp_handler()

def load_process_keywords():
	lmtzr = WordNetLemmatizer()
	with open('DrugsKeywords.txt', 'r', encoding='utf-8') as inf:
		lines = inf.read().split()
	word_set = set()
	word_set_short = set()
	for line in lines:
		word = line.split('\t')[0]
		word_lemm = lmtzr.lemmatize(word)
		if len(word) > 3:
			word_set.add(word)
		else:
			word_set_short.add(word)
		# only include the lemm word if it is not an subset of the word
		if word_lemm not in word:
			if len(word_lemm) > 3:
				word_set.add(word_lemm)
			else:
				word_set_short.add(word_lemm)
	return word_set, word_set_short

def compile_re():
	html_entities_re = r"&#?\w+;"
	quote_s_re = "'s|'t|'m"
	non_char_re = "[^ 0-9a-zA-Z-]"
	html_compiled = re.compile(html_entities_re)
	space_replace_compiled = re.compile('|'.join([quote_s_re, non_char_re]))
	# single_char_compiled = re.compile(r"(?:\b\w\b)+")
	# repeating_compiled = re.compile(r"([a-zA-Z])\1\1+")
	return html_compiled, space_replace_compiled #, repeating_compiled, single_char_compiled

# =============================================

def filter_tweet(tweet, word_set, word_set_short, stopwords, html_re, space_replace_re):
	if tweet['lang'] != 'en' and tweet['lang'] != 'und':
		# print(t['lang'])
		return False
	if 'extended_tweet' in tweet:
		text = tweet['extended_tweet']['full_text']
	else:
		text = tweet['text']
	# remove html elements
	text = html_re.sub(' ', text)
	# tokenize
	text_tokenized = preprocessor.tokenize(text)
	# lower case
	text_tokenized = text_tokenized.lower().replace('\n',' ')
	# remove non-chars
	text_tokenized = space_replace_re.sub(' ', text_tokenized)
	# split into token set
	tokens = [token for token in text_tokenized.split() if token not in stopwords]
	if len(tokens) > 0:
		content = ' '.join(tokens)
		token_set = set(tokens)
		# print(token_set)
	else:
		# skip match if no word left
		# print('no word left')
		return False
	
	match = False
	# for short words, do exact matching
	if len(word_set_short & token_set) > 0:
		match = True

	# for longer words, do fuzzy matching
	scores = [fuzz.partial_ratio(w, content) for w in word_set]
	if max(scores) >= 95:
		match = True

	return match

# worker process that filter tweets
def filter_data(path, out_path, word_set, word_set_short, stopwords, year):
	# TODO: add count of raw tweets
	html_compiled, space_replace_compiled = compile_re()

	raw_lines = []
	out_list = []

	try:
		if str(year) == '2017':
			tar = tarfile.open(path, mode='r:gz')
			names = tar.getnames()
			raw_lines = tar.extractfile(names[0]).readlines()
		else:
			raw_lines = gzip.open(path).readlines()
	except:
		logging.error("pid: {}, error opening file: {}".format(os.getpid(), path))
		return

	count = 0
	print('started scanning {}'.format(path))
	for l in raw_lines:
		try:
			tweet_str = codecs.decode(l)
			tweet = json.loads(tweet_str)
			flag = filter_tweet(tweet, word_set, word_set_short, stopwords, html_compiled, space_replace_compiled)
			count +=1
			if flag:
				out_list.append(tweet_str)

			if (count % 2500) == 0:
				looging.info('scanned {} for {}'.format(count, path))
		except:
			logging.error("pid: {}, error processing file: {}".format(os.getpid(), path))
			continue
	# output the results
	with open(out_path, 'w', encoding='utf-8') as outf:
		outf.writelines(out_list)
	logging.info('File {} filter done with {} tweets filtered'.format(path, len(out_list)))

# calls each process
def filter_all_lyrics(base_path, output_basepath, year, debug=True):
	# prepare path
	# base_path = base_path.format(year)
	# output_basepath = output_basepath.format(year)
	if not os.path.isdir(output_basepath):
		os.mkdir(output_basepath)

	# read keywords
	word_set, word_set_short = load_process_keywords()

	# read stopwords
	with open("custom_stopwords2.txt", 'r', encoding='utf-8') as inf:
		lines = inf.readlines()
		stopwords = set([line.strip() for line in lines])
	stopwords.add('URL')
	stopwords.add('EMOJI')
	stopwords.add('url')
	stopwords.add('emoji')

	filtered_files_list = []
	with open("finished_filtered.txt", 'r', encoding='utf-8') as filteredfiles:
		filtered_files_list = filteredfiles.read().splitlines()
	
	# prepare path list
	files = os.listdir(base_path)

	input_path_list = []
	output_path_list = []
	for f in files:
		if f not in filtered_files_list: 
			input_path_list.append(os.path.join(base_path, f))
			output_path_list.append(os.path.join(output_basepath, f.split('.')[0] + '.json'))

	if debug:
		input_path_list = input_path_list[0:10]
		output_path_list = output_path_list[0:10]
	
	
	
	# print(path_list)
	logging.info('input path list: {}'.format(str(input_path_list)))
	logging.info('output path list: {}'.format(str(output_path_list)))

	# build param list
	param_list = [(input_path_list[i], output_path_list[i], word_set, word_set_short, stopwords, year) for i in range(len(input_path_list))]

	# Serial process
	for i in range(len(input_path_list)):
		filter_data(input_path_list[i], output_path_list[i], word_set, word_set_short, stopwords, year)
	
	# do multi-process
	#with Pool(processes=10) as pool:
	#	pool.starmap(filter_data, param_list)

	return

if __name__ == '__main__':
	# import_keywords()
	# logging.info('Main process: {} started'.format(os.getpid()))
	filter_all_lyrics(basepath, output_basepath, year, debug=False)
