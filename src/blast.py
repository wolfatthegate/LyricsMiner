## https://github.com/JiaShun-Xiao/BLAST-bioinfor-tool/blob/master/blast.py
# compare single base
import nltk
import pandas as pd
import TextCleaner
import logging

class blast:
    logFormatter = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(filename='logs/myLogs.log',level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

    def SingleBaseCompare(self, char1,char2):
        if char1 == char2:
            return 2
        else:
            return -1
        
    def W2WCompare(self, word1, word2, threshold):
        result = blast.SMalignmentGlobal(self, word1.lower(), word2.lower())
        if result[2] > threshold:
            return 2
        else: 
            return -1
    
    def SMWalignment(self, seq1, seq2, threshold):
        
        cleaner = TextCleaner.TextCleaner()
        seq1 = cleaner.clean(seq1)
        seq2 = cleaner.clean(seq2)
        
        seq1 = '~~ ' + seq1 + ' ~~'
        seq2 = '^^ ' + seq2 + ' ^^'
        
        seq1_tokens = nltk.word_tokenize(seq1)
        seq2_tokens = nltk.word_tokenize(seq2)
        
        if len(seq1_tokens) < 3 or len(seq2_tokens) < 3:
            return '', '', 0, 0, 0, 0, False, False
        
        m = seq1_tokens.__len__()
        n = seq2_tokens.__len__()
        g = -3
        
        matrix = []
        matchMap = []
        matchCounter = 0
        
        for i in range(0, m):
            tmp = []
            for j in range(0, n):
                tmp.append(0)
            matrix.append(tmp)
            
        for i in range(0, m):
            tmp = []
            for j in range(0, n):
                tmp.append(0)
            matchMap.append(tmp)
            
        for sii in range(0, m):
            matrix[sii][0] = sii*-3
             
        for sjj in range(0, n):
            matrix[0][sjj] = sjj*-3
        
        for siii in range(1, m):
            for sjjj in range(1, n):
                matchScore = blast.W2WCompare(self, seq1_tokens[siii], seq2_tokens[sjjj],threshold)
                
                leftCell = matrix[siii][sjjj-1] + g
                upperCell = matrix[siii-1][sjjj] + g
                cornerCell = matrix[siii - 1][sjjj - 1] + matchScore  
                     
                matrix[siii][sjjj] = max(leftCell, upperCell, cornerCell)
                              
                if matchScore is 2: 
                    matchMap[siii][sjjj] = 2
                    matchCounter += 1
                else:
                    matchMap[siii][sjjj] = 0
        
        if matchCounter is 0:
            return 'no match found', '', 0, 0, 0, 0, False, False
        
#         df = pd.DataFrame(matrix)
#         print(df)
        
        sequ1 = []
        sequ2 = []
        
        m = m - 1
        n = n - 1
                
        while m > 0 and n > 0:
         
            if max(matrix[m][n-1], matrix[m-1][n-1], matrix[m-1][n]) == matrix[m-1][n-1]:
                if matchMap[m-1][n-1] is 2: 
                    sequ1.append(seq1_tokens[m-1] + ' ')
                    sequ2.append(seq2_tokens[n-1] + ' ')
                else: 
                    sequ1.append(seq1_tokens[m-1] + ' ')
                    sequ2.append(blast.fillspace(self, len(seq1_tokens[m-1])) + ' ')              
                m -= 1
                n -= 1
                  
            elif max(matrix[m][n-1], matrix[m-1][n-1], matrix[m-1][n]) == matrix[m][n-1]:     
                sequ1.append(blast.fillspace(self, len(seq2_tokens[n-1])) + ' ')
                sequ2.append(seq2_tokens[n-1] + ' ')
                n -= 1
            else:               
                sequ1.append(seq1_tokens[m-1] + ' ')
                sequ2.append(blast.fillspace(self, len(seq1_tokens[m-1])) + ' ')
                m -= 1
               
        sequ1.reverse()
        sequ2.reverse()
        
        align_seq1 = ''.join(sequ1[1:])
        align_seq2 = ''.join(sequ2[1:])
        align_counter = 0
        first_half = 0
        second_half = 0
        first_half_score = 0
        second_half_score = 0
        sequential_search_recommandation = False
        stepback_search_recommandation = False
        
        for k in range(0, len(sequ1)):
            if blast.W2WCompare(self, sequ1[k], sequ2[k], threshold) is 2:
                align_counter += 1 
                
        align_score = float(align_counter)/(len(sequ1)-1)
        
        # if alignment score is greater than 0.49, we don't need to search any farther 
        if align_score > 0.49: 
            return align_seq1, align_seq2, align_score, align_counter, first_half_score, second_half_score, sequential_search_recommandation, stepback_search_recommandation
        
        greterLen = max(len(sequ1), len(sequ2))
        halfway = int(greterLen/2)
        
        for k in range(0, halfway):
            if blast.W2WCompare(self, sequ1[k], sequ2[k], threshold) is 2:
                first_half += 1
                
        for k in range(halfway, len(sequ1)):
            if blast.W2WCompare(self, sequ1[k], sequ2[k], threshold) is 2:
                second_half += 1
        
        try:         
            first_half_score = float(first_half)/(halfway - 1)
            second_half_score = float(second_half)/(len(sequ1) - halfway)
        except:
            logging.warning('float division by zero')
        
        if (first_half_score > 0.49 or align_counter > 4)and second_half_score < .35:
            sequential_search_recommandation = True
        
        if first_half_score < 0.15 and (second_half_score > .45 or align_counter > 4):
            stepback_search_recommandation = True
            
        
        return align_seq1, align_seq2, align_score, align_counter, first_half_score, second_half_score, sequential_search_recommandation, stepback_search_recommandation
    
    
    ### One can also pass two sets of tokens for searching approximate match
    
    def SMTalignment(self, tok1, tok2, threshold):
        
        if len(tok1) < 1 or len(tok2) < 1:
            return '', '', 0, 0
        
        seq1_tokens = tok1
        seq2_tokens = tok2
        
        seq1_tokens.append('~~')
        seq1_tokens.insert(0, '~~')
        
        seq2_tokens.append('^^')
        seq2_tokens.insert(0, '^^') 
        
        m = seq1_tokens.__len__()
        n = seq2_tokens.__len__()
        g = -3
        
        matrix = []
        matchMap = []
        matchCounter = 0
        
        for i in range(0, m):
            tmp = []
            for j in range(0, n):
                tmp.append(0)
            matrix.append(tmp)
            
        for i in range(0, m):
            tmp = []
            for j in range(0, n):
                tmp.append(0)
            matchMap.append(tmp)
            
        for sii in range(0, m):
            matrix[sii][0] = sii*-3
             
        for sjj in range(0, n):
            matrix[0][sjj] = sjj*-3
        
        for siii in range(1, m):
            for sjjj in range(1, n):
                matchScore = blast.W2WCompare(self, seq1_tokens[siii], seq2_tokens[sjjj],threshold)
                
                leftCell = matrix[siii][sjjj-1] + g
                upperCell = matrix[siii-1][sjjj] + g
                cornerCell = matrix[siii - 1][sjjj - 1] + matchScore  
                     
                matrix[siii][sjjj] = max(leftCell, upperCell, cornerCell)
                              
                if matchScore is 2: 
                    matchMap[siii][sjjj] = 2
                    matchCounter += 1
                else:
                    matchMap[siii][sjjj] = 0
        
        if matchCounter is 0:
            seq1_tokens.pop()
            seq1_tokens.pop(0)
        
            seq2_tokens.pop()
            seq2_tokens.pop(0) 
            
            return 'no match found', '', 0, 0
        
        sequ1 = []
        sequ2 = []
        
        m = m - 1
        n = n - 1
                
        while m > 0 and n > 0:
         
            if max(matrix[m][n-1], matrix[m-1][n-1], matrix[m-1][n]) == matrix[m-1][n-1]:
                if matchMap[m-1][n-1] is 2: 
                    sequ1.append(seq1_tokens[m-1] + ' ')
                    sequ2.append(seq2_tokens[n-1] + ' ')
                else: 
                    sequ1.append(seq1_tokens[m-1] + ' ')
                    sequ2.append(blast.fillspace(self, len(seq1_tokens[m-1])) + ' ')              
                m -= 1
                n -= 1
                  
            elif max(matrix[m][n-1], matrix[m-1][n-1], matrix[m-1][n]) == matrix[m][n-1]:     
                sequ1.append(blast.fillspace(self, len(seq2_tokens[n-1])) + ' ')
                sequ2.append(seq2_tokens[n-1] + ' ')
                n -= 1
            else:               
                sequ1.append(seq1_tokens[m-1] + ' ')
                sequ2.append(blast.fillspace(self, len(seq1_tokens[m-1])) + ' ')
                m -= 1
               
        sequ1.reverse()
        sequ2.reverse()
        
        align_seq1 = ''.join(sequ1[1:])
        align_seq2 = ''.join(sequ2[1:])
        align_counter = 0
        
        for k in range(0, len(sequ1)):
            if blast.W2WCompare(self, sequ1[k], sequ2[k], threshold) is 2:
                align_counter += 1 
                
        align_score = float(align_counter)/(len(sequ1)-1)
                
        seq1_tokens.pop()
        seq1_tokens.pop(0)
        
        seq2_tokens.pop()
        seq2_tokens.pop(0)
        return align_seq1, align_seq2, align_score, align_counter
    
    # Smith–Waterman Alignment 
    # Use it for word to word comparison
    
    def SMalignmentGlobal(self, seq1, seq2):
        m = len(seq1)
        n = len(seq2)
        g = -3
        matrix = []
        for i in range(0, m):
            tmp = []
            for j in range(0, n):
                tmp.append(0)
            matrix.append(tmp)
        for sii in range(0, m):
            matrix[sii][0] = sii*g
        for sjj in range(0, n):
            matrix[0][sjj] = sjj*g
        for siii in range(1, m):
            for sjjj in range(1, n):
                upperCell = matrix[siii-1][sjjj] + g
                leftCell = matrix[siii][sjjj-1] + g
                cornerCell = matrix[siii - 1][sjjj - 1] + blast.SingleBaseCompare(self, seq1[siii],seq2[sjjj])
                matrix[siii][sjjj] = max(upperCell, leftCell, cornerCell)
        sequ1 = [seq1[m-1]]
        sequ2 = [seq2[n-1]]
        while m > 1 and n > 1:
            if max(matrix[m-1][n-2], matrix[m-2][n-2], matrix[m-2][n-1]) == matrix[m-2][n-2]:
                m -= 1
                n -= 1
                sequ1.append(seq1[m-1])
                sequ2.append(seq2[n-1])
            elif max(matrix[m-1][n-2], matrix[m-2][n-2], matrix[m-2][n-1]) == matrix[m-1][n-2]:
                n -= 1
                sequ1.append('-')
                sequ2.append(seq2[n-1])
            else:
                m -= 1
                sequ1.append(seq1[m-1])
                sequ2.append('-')
        sequ1.reverse()
        sequ2.reverse()
        align_seq1 = ''.join(sequ1)
        align_seq2 = ''.join(sequ2)
        align_score = 0.
        
        for k in range(0, len(align_seq1)):
            if align_seq1[k] == align_seq2[k]:
                align_score += 1
        align_score = float(align_score)/len(align_seq1)
        return align_seq1, align_seq2, align_score
    
    def fillspace(self, length):
        s = ''
        for x in range(0, length): 
            s = s + '-'
        return s
            
