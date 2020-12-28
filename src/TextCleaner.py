#
# TextClearner removes twitter users, urls, hashtabs, special characters and emojis. 
# 

import re

class TextCleaner:
    def clean(self, string):
            
        user_pattern = re.compile('@\w+')
        url_pattern = re.compile('(https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|www\.[a-zA-Z0-9][a-zA-Z0-9-]+[a-zA-Z0-9]\.[^\s]{2,}|https?:\/\/(?:www\.|(?!www))[a-zA-Z0-9]+\.[^\s]{2,}|www\.[a-zA-Z0-9]+\.[^\s]{2,})')
        hashtag_pattern = re.compile('#[a-zA-Z0-9-]+')
        spchar_pattern = re.compile('[!*?\/()\[\],:.;&~]')
        spchar_pattern2 = re.compile('[%\|•‚\-’"]')
        other_pattern = re.compile('\"')
        white_space = re.compile('\s+')
        emoji_pattern = re.compile("["
                        u"\U0001F600-\U0001F64F"  # emoticons
                    u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                    u"\U0001F680-\U0001F6FF"  # transport & map symbols
                    u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                    u"\U00002702-\U000027B0"
                    u"\U000024C2-\U0001F251"
                    u"\U0001f926-\U0001f937"
                    u'\U00010000-\U0010ffff'
                    u"\u200d"
                    u"\u2640-\u2642"
                    u"\u2600-\u2B55"
                    u"\u23cf"
                    u"\u23e9"
                    u"\u231a"
                    u"\u3030"
                    u"\ufe0f"
        "]+")
        try: 

            string = emoji_pattern.sub(r'', string)         
            string = user_pattern.sub(r'', string) 
            string = url_pattern.sub(r'', string)
            string = hashtag_pattern.sub(r'', string)       
            string = spchar_pattern.sub(r' ', string)
            string = spchar_pattern2.sub(r' ', string)
            string = other_pattern.sub(r'', string)
            string = white_space.sub(r' ', string)
            string = string.replace('\'', '')
            string = string.replace('\\', '')
            string = string.replace('+', '')
            string = string.replace('\n', ' ')
            string = string.replace('eee', 'ee')
            string = string.replace('hh', 'h')
            string = string.strip()
        except:
            print(str(string) + ' some text cannot be cleaned')
        return string