
# coding: utf-8

# In[298]:

import sys
import re
import string
import csv
import random
import time
#import binascii
#import shlex
import numpy as np
from pandas import read_csv, DataFrame
from itertools import groupby
from operator import itemgetter
from collections import Iterable, OrderedDict
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.corpus import stopwords
from scipy import stats
#from datasketch import MinHash, MinHashLSH
import NE_candidate_module as ne
import NE_candidate_module as ne
import Mention
import threading, queue


# In[324]:

#---------------------Existing Lists--------------------
cachedStopWords = set(stopwords.words("english"))
cachedStopWords.update(("and","or","other","another","nor","sometimes","sometime","because","can","like","into","able","unable","either","neither","if","we","it","else","elsewhere","how","not","what","who","when","where","who's","who’s","let","today","tonight","let's","let’s","lets","know","make","oh","via","i","yet","must","mustnt","mustn't","mustn’t","i'll","i’ll","done","doesnt","doesn't","doesn’t","dont","don't","don’t","did","didnt","didn't","didn’t","much","without","could","couldn't","couldn’t","would","wouldn't","wouldn’t","should","shouldn't","souldn’t","shall","isn't","isn’t","hasn't","hasn’t","wasn't","wasn’t","also","with","let's","let’s","let","well","just","everyone","anyone","noone","none","someone","theres","there's","there’s","everybody","nobody","somebody","anything","else","elsewhere","something","nothing","everything","i'd","i’d","i’m","won't","won’t","i’ve","i've","they're","they’re","we’re","we're","we'll","we’ll","we’ve","we've","they’ve","they've","they’d","they'd","they’ll","they'll","again","you're","thats","that's",'that’s','here’s',"here's","what's","what’s","i'm","a","so","except","arn't","aren't","arent","this","when","it","it’s","it's","he's","she's","she'd","he'd","he'll","she'll","she’ll","many","can't","cant","can’t","even","yes","no","these","here","there","to","may","maybe","<hashtag>","<hashtag>.","ever","every","never","there's","there’s","whenever","wherever","however","whatever","always"))
cachedStopWords.discard("don")
cachedTitles = ["mr.","mr","mrs.","mrs","miss","ms","sen.","dr.","prof.","president","congressman"]
prep_list=["in","at","of","on","by","&;"] #includes common conjunction as well
article_list=["a","an","the"]
day_list=["sunday","monday","tuesday","wednesday","thursday","friday","saturday","mon","tues","wed","thurs","fri","sat","sun"]
month_list=["january","february","march","april","may","june","july","august","september","october","november","december","jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"]
chat_word_list=["please","cuz","coz","sorry","ur","thank","thanks","congrats","whoa","ha","ok","okay","hey","hi","huh","ya","yep","yeah","fyi","duh","damn","lol","omg","congratulations","fuck","wtf","wtaf","xoxo","rofl","imo","wow","fck","haha","hehe","hoho"]

#string.punctuation.extend('“','’','”')
#---------------------Existing Lists--------------------


# In[300]:

class SatadishaModule():

    def __init__(self,batch):
        self.batch=batch
        return


    def flatten(self,mylist, outlist,ignore_types=(str, bytes, ne.NE_candidate)):
    
        if mylist !=[]:
            for item in mylist:
                #print not isinstance(item, ne.NE_candidate)
                if isinstance(item, list) and not isinstance(item, ignore_types):
                    self.flatten(item, outlist)
                else:
                    if isinstance(item,ne.NE_candidate):
                        item.phraseText=item.phraseText.strip(' \t\n\r')
                        item.reset_length()
                    else:
                        item=item.strip(' \t\n\r')
                    outlist.append(item)
        return outlist

    def extract(self): 
        #df = read_csv('eric_trump.csv', index_col='ID', header=0, encoding='utf-8')
        print("extracting now")

        #output.csv
        #df_out= DataFrame(columns=('tweetID', 'sentID', 'hashtags', 'user', 'usertype', 'TweetSentence', 'phase1Candidates'))
        df_out= DataFrame(columns=('tweetID', 'sentID', 'hashtags', 'user', 'TweetSentence', 'phase1Candidates'))

        #%%timeit -o
        #module_capital_punct.main:
        '''I am running this for 100 iterations for testing purposes. Of course you no longer need this for loop as you are
        #running one tuple at a time'''
        NE_container={}
        candidateBase={}
        #NE_container=DataFrame(columns=('candidate', 'frequency', 'capitalized', 'start_of_sentence', 'abbreviation', 'all_capitalized','is_csl','title','has_number','date_indicator','is_apostrophed','has_intermediate_punctuation','ends_like_verb','ends_like_adverb','change_in_capitalization','has_topic_indicator'))

        count=0
        ne_count=0
        userMention_count=0
        token_count=0

        NE_list_phase1=[]
        UserMention_list=[]
        ME_EXTR=Mention.Mention_Extraction()

        #--------------------------------------PHASE I---------------------------------------------------
        for index, row in self.batch.iterrows():
            
            #hashtags=str(row['Discussion'])
            hashtags=str(row['HashTags'])
            user=str(row['User'])
            #userType=str(row['User Type'])
            tweetText=str(row['TweetText'])

            #print(str(index))

            #capitalization module
            #if all words are capitalized:
            if tweetText.isupper():
                print("",end="")
                #print ("All caps module\n")
                outrow=[str(index), str(0), hashtags, user, tweetText, ""]
                df_out.loc[len(df_out)]=outrow
            elif tweetText.islower():
                print("",end="")
                outrow=[str(index), str(0), hashtags, user, tweetText, ""]
                df_out.loc[len(df_out)]=outrow
                #print ("All lower module\n")
            else:
                ne_List_final=[]
                userMention_List_final=[]
                #pre-modification: returns word list split at whitespaces; retains punctuation
                tweetSentences=list(filter (lambda sentence: len(sentence)>1, tweetText.split('\n')))
                tweetSentenceList_inter=self.flatten(list(map(lambda sentText: sent_tokenize(sentText.lstrip().rstrip()),tweetSentences)),[])
                tweetSentenceList=list(filter (lambda sentence: len(sentence)>1, tweetSentenceList_inter))
                #quoteProcessedList=list(map (lambda sentence: quoteProcess(sentence), tweetSentenceList))

                for sen_index in range(len(tweetSentenceList)):
                    sentence=tweetSentenceList[sen_index]
                    #print(sentence)
                    #tweetWordList= list(filter(lambda word:(word.strip(string.punctuation))!="",sentence.split()))
                    
                    tweetWordList=sentence.split()

                    token_count+=len(tweetWordList)
                    #returns position of words that are capitalized
                    tweetWordList_cappos = list(map(lambda element : element[0], filter(lambda element : self.capCheck(element[1]), enumerate(tweetWordList))))
                  
                    #returns list of stopwords in tweet sentence
                    combined_list_here=([]+list(cachedStopWords)+prep_list+chat_word_list)
                    combined_list_here.remove("the")
                    tweetWordList_stopWords=list(filter (lambda word: ((((word.strip()).strip(string.punctuation)).lower() in combined_list_here)|(word.strip() in string.punctuation)|(word.startswith('@'))), tweetWordList))
                    #if((len(tweetWordList))==(len(tweetWordList_cappos)+len(tweetWordList_stopWords))):
                        #print(sentence)
                    
                    #returns list of @userMentions
                    userMentionswPunct=list(filter(lambda phrase: phrase.startswith('@'), tweetWordList))
                    userMentions=list(map(lambda mention: mention.rstrip(string.punctuation), userMentionswPunct))
                    
                    userMention_count+=len(userMentions)
                    userMention_List_final+=userMentions                
                    #function to process and store @ user mentions---- thread 1
                    threading.Thread(target=ME_EXTR.ComputeAll, args=(userMention_List_final,)).start()

                    #non @usermentions are processed in this function to find non @, non hashtag Entities---- thread 2
                    ne_List_allCheck=[]
                    if((len(tweetWordList))>(len(tweetWordList_cappos)+len(tweetWordList_stopWords))):
                        #print(str(len(tweetWordList_cappos))+" "+str(len(tweetWordList_stopWords)))
                        q = queue.Queue()
                        threading.Thread(target=self.trueEntity_process, args=(tweetWordList_cappos,tweetWordList,q)).start()
                        #ne_List_allCheck= trueEntity_process(tweetWordList_cappos,tweetWordList)
                        ne_List_allCheck= q.get()

                    ne_count+=len(ne_List_allCheck)
                    ne_List_final+=ne_List_allCheck

                    #write row to output dataframe
                    phase1Out=""
                    for candidate in ne_List_allCheck:
                        phase1Out+=(((candidate.phraseText).lstrip(string.punctuation)).strip())+"|| " 
                    outrow=[str(index), str(sen_index), hashtags, user, sentence, phase1Out]
                    df_out.loc[len(df_out)]=outrow

                for candidate in ne_List_final:
                    self.insert_dict (candidate,NE_container,candidateBase,index,sen_index)
                #self.printList(ne_List_final)
                #if(userMention_List_final):
                #    print(userMention_List_final)

                NE_list_phase1+=ne_List_final
                UserMention_list+=userMention_List_final
                #print ("\n")

        #unmerged candidate table and candidateBase
        sorted_NE_container =OrderedDict(sorted(NE_container.items(), key=lambda t: t[1],reverse=True))
        sorted_candidateBase =OrderedDict(sorted(candidateBase.items(), key=lambda t: len(t[1]),reverse=True))

        #computing z-score of frequency
        frequency_array = np.array(list(map(lambda val: val[0], sorted_NE_container.values())))
        zscore_array=stats.zscore(frequency_array)

        index=0
        #for key, val in sorted_NE_container:
        #fieldnames=['candidate','freq','length','cap','start_of_sen','abbrv','all_cap','is_csl','title','has_no','date','is_apostrp','has_inter_punct','ends_verb','ends_adverb','change_in_cap','topic_ind','@mention','z_score']

        for key in sorted_NE_container.keys():
            #alias=computeAlias(key) #+[str(zscore_array[index])]
            val=sorted_NE_container[key]+[str(ME_EXTR.checkInDictionary(key))]
            index+=1
            sorted_NE_container[key]=val


            #print (key+" : "+str(sorted_NE_container[key]))

        #with open('candidates500.csv', 'w') as output_candidate:
        # with open('candidates.csv', 'w') as output_candidate:
        #     writer = csv.writer(output_candidate)
        #     writer.writerow(fieldnames)
        #     for k, v in sorted_NE_container.items():
        #         writer.writerow([k] + v)

        #ME_EXTR.PrintDictionary()
        print("**********************************************************")

        print("Total number of tokens processed: "+str(token_count))
        print ("Total number of candidate NEs extracted: "+str(len(sorted_NE_container.keys())))

        #df_out.to_csv('TweetBase.csv')
        return sorted_NE_container

    
    def rreplace(self,s, old, new, occurrence):
        if s.endswith(old):
            li = s.rsplit(old, occurrence)
            return new.join(li)
        else:
            return s


# In[301]:

#candidate: 'frequency','length', 'capitalized', 'start_of_sentence', 'abbreviation', 'all_capitalized','is_csl','title','has_number','date_indicator','is_apostrophed','has_intermediate_punctuation','ends_like_verb','ends_like_adverb','change_in_capitalization','has_topic_indicator'
    def insert_dict(self,candidate,NE_container,candidateBase,tweetID,sentenceID):
        #if ((candidate.phraseText.startswith('"'))|(candidate.phraseText.endswith('"'))):
        
        key=(((candidate.phraseText.lstrip(string.punctuation)).rstrip(string.punctuation)).strip()).lower()
        key=(key.lstrip('“‘’”')).rstrip('“‘’”')
        key= self.rreplace(self.rreplace(self.rreplace(key,"'s","",1),"’s","",1),"’s","",1)
        
        if key in NE_container:
            feature_list=NE_container[key]
            feature_list[0]+=1
            for index in [0,1,2,3,4,5,6,7,9,10,11,13]:
                if (candidate.features[index]==True):
                    feature_list[index+2]+=1
            for index in [8,12]:
                if (candidate.features[index]!=-1):
                    feature_list[index+2]+=1
        else:
            feature_list=[0]*16
            feature_list[0]+=1
            feature_list[1]=candidate.length
            #call background process to check for non capitalized occurences
            for index in [0,1,2,3,4,5,6,7,9,10,11,13]:
                if (candidate.features[index]==True):
                    feature_list[index+2]+=1
            for index in [8,12]:
                if (candidate.features[index]!=-1):
                    feature_list[index+2]+=1
            NE_container[key] = feature_list
        
        #insert in candidateBase
        if key in candidateBase.keys():
            candidateBase[key]=candidateBase[key]+[str(tweetID)+":"+str(sentenceID)]
        else:
            candidateBase[key]=[str(tweetID)+":"+str(sentenceID)]
        
        return


# In[302]:

    def printList(self,mylist):
        print("["),
        #print "[",
        for item in mylist:
            if item != None:
                if isinstance(item,ne.NE_candidate):
                    item.print_obj()
                    #print (item.phraseText)
                else:
                    print (item+",", end="")
                    #print item+",",
        #print "]"
        print("]")
        return


# In[303]:




# In[304]:

    def consecutive_cap(self,tweetWordList_cappos,tweetWordList):
        output=[]
        #identifies consecutive numbers in the sequence
        for k, g in groupby(enumerate(tweetWordList_cappos), lambda element: element[0]-element[1]):
            output.append(list(map(itemgetter(1), g)))
        count=0
        if output:        
            final_output=[output[0]]
            for first, second in (zip(output,output[1:])):
                #print(tweetWordList[first[-1]])
                if ((not (tweetWordList[first[-1]]).endswith('"'))&((second[0]-first[-1])==2) & (tweetWordList[first[-1]+1].lower() in prep_list)):
                    (final_output[-1]).extend([first[-1]+1]+second)
                elif((not (tweetWordList[first[-1]].endswith('"')))&((second[0]-first[-1])==3) & (tweetWordList[first[-1]+1].lower() in prep_list)& (tweetWordList[first[-1]+2].lower() in article_list)):
                    (final_output[-1]).extend([first[-1]+1]+[first[-1]+2]+second)
                else:
                    final_output.append(second)
                    #merge_positions.append(False)
        else:
            final_output=[]
        
        return final_output


# In[305]:

#basically splitting the original NE_candidate text and building individual object from each text snippet
    def build_custom_NE(self,phrase,pos,prototype,feature_index,feature_value):
        #print("Enters")
        position=pos
        custom_NE= ne.NE_candidate(phrase,position)
        for i in range(14):
            custom_NE.set_feature(i,prototype.features[i])
        custom_NE.set_feature(feature_index,feature_value)
        if (feature_index== ne.is_csl) & (feature_value== True):
            custom_NE.set_feature(ne.start_of_sentence, False)
        custom_NE=self.entity_info_check(custom_NE)
        return custom_NE


    # In[306]:

    def abbrv_algo(self,ne_element):
        '''abbreviation algorithm 
        trailing apostrophe:
               |period:
               |     multiple letter-period sequence:
               |         all caps
               | non period:
               |     ?/! else drop apostrophe
        else:
            unchanged
        '''
        phrase= ne_element.phraseText
        #print("=>"+phrase)
        #since no further split occurs we can set remaining features now
        ne_element.set_feature(ne.capitalized, True)
        if ne_element.phraseText.isupper():
            ne_element.set_feature(ne.all_capitalized, True)
        else:
            ne_element.set_feature(ne.all_capitalized, False)
            
        abbreviation_flag=False
        p=re.compile(r'[^a-zA-Z\d\s]$')
        match_list = p.findall(phrase)
        if len(match_list)>0:
            #print("Here")
            if phrase.endswith('.'):
                p1= re.compile(r'([a-zA-Z][\.]\s*)')
                match_list = p1.findall(phrase)
                if ((len(match_list)>1) & (len(phrase)<6)):
                    #print ("1. Found abbreviation: "+phrase)
                    abbreviation_flag= True
                else:
                    phrase= phrase[:-1]
            else:
                phrase= phrase[:-1]
                #if not (phrase.endswith('?')|phrase.endswith('!')|phrase.endswith(')')|phrase.endswith('>')):
                    #phrase= phrase[:-1]
        else:
            p2=re.compile(r'([^a-zA-Z0-9_\s])')
            match_list = p2.findall(phrase)
            if ((len(match_list)==0) & (phrase.isupper()) & (len(phrase)<7)& (len(phrase)>1)):
                #print ("2. Found abbreviation!!: "+phrase)
                abbreviation_flag= True
            else:
                #print("Here-> "+phrase)
                p3= re.compile(r'([A-Z][.][A-Z])')
                p4= re.compile(r'\s')
                match_list = p3.findall(phrase)
                match_list1 = p4.findall(phrase)
                if ((len(match_list)>0) & (len(match_list1)==0)):
                    abbreviation_flag= True
                    #print ("3. Found abbreviation!!: "+phrase)
                
        #element= ne.NE_candidate(phrase.strip())
        ne_element.phraseText=phrase
        ne_element.reset_length()
        ne_element.set_feature(ne.abbreviation, abbreviation_flag)
        return ne_element
        


    # In[307]:

    def punct_clause(self,NE_phrase_in):
        
        NE_phrases=self.entity_info_check(NE_phrase_in)
        cap_phrases=NE_phrases.phraseText.strip()
        final_lst=[]
        #print ("++"+cap_phrases)
        if (re.compile(r'[^a-zA-Z0-9_\s]')).findall(cap_phrases):
            #case of intermediate punctuations: handles abbreviations
            p1= re.compile(r'(?:[a-zA-Z0-9][^a-zA-Z0-9_\s]\s*)')
            match_lst = p1.findall(cap_phrases)
            #print(match_lst)
            if match_lst:
                index= (list( p1.finditer(cap_phrases) )[-1]).span()[1]
            
            p= re.compile(r'[^a-zA-Z\d\s]')
            match_list = p.findall(cap_phrases)

            p2=re.compile(r'[^a-zA-Z\d\s]$') #ends with punctuation

            if len(match_list)-len(match_lst)>0:
                if (p2.findall(cap_phrases)):
                    #only strips trailing punctuations, not intermediate ones following letters
                    cap_phrases = cap_phrases[0:index]+re.sub(p, '', cap_phrases[index:])
                    NE_phrases.phraseText= cap_phrases
            
        
        #comma separated NEs
        #lst=filter(lambda(word): word!="", re.split('[,]', cap_phrases))
        #print ("=>"+ cap_phrases)
        start_of_sentence_fix=NE_phrases.features[ne.start_of_sentence]
        wordlst=list(filter(lambda word: word!="", re.split('[,:!…..]', cap_phrases)))
        #print(wordlst)
        if (NE_phrases.features[ne.date_indicator]==False) & (len(wordlst)>1):
            pos=NE_phrases.position
            combined=[]
            prev=0
            for i in range(len(wordlst)):
                word=wordlst[i]
                word_len=len(list(filter(lambda individual_word: individual_word!="", re.split('[ ]', word))))
                word_pos=pos[(prev):(prev+word_len)]
                prev=prev+word_len
                combined+=[[word]+word_pos]
            
            lst_nsw=list(filter(lambda element: (((str(element[0])).lower() not in cachedStopWords) & (len(str(element[0]))>1)) ,combined))
            #print (lst_nsw)
            if(lst_nsw):
                final_lst= list(map(lambda element:self.build_custom_NE(str(element[0]),element[1:],NE_phrases,ne.is_csl,True), lst_nsw))
                final_lst[0].set_feature(ne.start_of_sentence, NE_phrases.features[ne.start_of_sentence])
        else:
            NE_phrases.set_feature(ne.is_csl,False)
            final_lst=[NE_phrases]
        
        #check abbreviation
        if(final_lst):
            final_lst= list(map(lambda phrase: self.abbrv_algo(phrase), final_lst))

        
        #print(lst)
        return final_lst


    # In[308]:

    #%%timeit -o
    def f(self,x,tweetWordList):

        #list1=map(lambda word: check(tweetWordList[word], word), x)
        list1=list(map(lambda word: tweetWordList[word]+" ", x[:-1]))
        phrase="".join(list1)+tweetWordList[x[-1]]

        if not ((phrase[0].isdigit()) & (len(x)==1)):
            NE_phrase= ne.NE_candidate(phrase.strip(),x)
            if 0 in x:
                NE_phrase.set_feature(ne.start_of_sentence,True)
            else:
                NE_phrase.set_feature(ne.start_of_sentence,False)
        else:
            NE_phrase= ne.NE_candidate("JUST_DIGIT_ERROR",[])

        return NE_phrase


    # In[309]:

    def capCheck(self,word):
        combined_list=[]+list(cachedStopWords)+prep_list+chat_word_list
        if word.startswith('@'):
            return False
        elif "<Hashtag" in word:
            return False
        elif (((word.strip('“‘’”')).lstrip(string.punctuation)).rstrip(string.punctuation)).lower() in combined_list:
            if((word=="The")|(word=="THE")):
                return True
            else:
                return False
        elif word[0].isdigit():
            return True
        else:
            p=re.compile(r'^[\W]*[A-Z]')
            l= p.match(word)
            if l:
                return True
            else:
                return False


    # In[310]:

    def title_check(self,ne_phrase):
        title_flag=False
        words=ne_phrase.phraseText.split()
        for word in words:
            if word.lower() in cachedTitles:
                title_flag= True
                break
        ne_phrase.set_feature(ne.title,title_flag)
        return ne_phrase


    # In[311]:

    def entity_info_check(self,ne_phrase):
        flag1=False #has number
        flag3=False
        flag_ind=[] #is number
        month_ind=[]
        date_num_holder=[]
        words=ne_phrase.phraseText.split()
        for word in words:
            word=(word.strip()).rstrip(string.punctuation).lower()
            punct_flag=False
            for char in word:
                if ((char in string.punctuation)|(char in ['“','’','”','…'])):
                    punct_flag=True
                    break
            #if ((not word.isalpha())& (not "'s" in word) & (not "’s" in word)):
            if ((not word.isalpha())& (not punct_flag)):
                flag_ind+=[True]
                if word.isdigit():
                    date_num_holder+=['num']
                else:
                    date_num_holder+=['alpha']
            else:
                flag_ind+=[False]
                if word in month_list:
                    month_ind+=[True]
                    date_num_holder+=['month']
                elif word in day_list:
                    date_num_holder+=['day']
                elif word in prep_list:
                    date_num_holder+=['preposition']
                elif word in article_list:
                    date_num_holder+=['article']
                else:
                    #print("=>"+word)
                    date_num_holder+=['string']
        if True in flag_ind:
            flag1=True
        if True in month_ind:
            flag3=True
        ne_phrase.set_feature(ne.has_number,flag1)
        ne_phrase.set_feature(ne.date_indicator,flag3)
        ne_phrase.set_date_num_holder(date_num_holder)
        return ne_phrase


    # In[312]:

    #removing commonly used expletives, enunciated chat words and other common words (like days of the week, common expressions)
    def slang_remove(self,ne_phrase):
        phrase=(ne_phrase.phraseText.strip()).rstrip(string.punctuation).lower()
        p1= re.compile(r'([A-Za-z]+)\1\1{1,}')
        match_lst = p1.findall(phrase)
        if phrase in article_list:
            return True
        elif phrase in day_list:
            return True
        elif phrase in month_list:
            return True
        elif match_lst:
            return True
        else:
            return False


    # In[313]:

    def apostrope_check(self,ne_phrase):
        apostrophe="'s"
        bad_apostrophe="’s"
        phrase=(ne_phrase.phraseText.strip()).rstrip(string.punctuation).lower()
        if (apostrophe in phrase):
            if (phrase.endswith(apostrophe)):
                ne_phrase.set_feature(ne.is_apostrophed,0)
            else:
                #print(phrase.find(apostrophe))
                ne_phrase.set_feature(ne.is_apostrophed,phrase.find(apostrophe))
        elif (bad_apostrophe in phrase):
            if phrase.endswith(bad_apostrophe):
                ne_phrase.set_feature(ne.is_apostrophed,0)
            else:
                #print(phrase.find(apostrophe))
                ne_phrase.set_feature(ne.is_apostrophed,phrase.find(bad_apostrophe))
        else:
            ne_phrase.set_feature(ne.is_apostrophed,-1)
        return ne_phrase


    # In[314]:

    def punctuation_check(self,ne_phrase):
        holder=[]
        punctuation_holder=[]
        flag_holder=[]
        phrase=(ne_phrase.phraseText.strip()).rstrip(string.punctuation).lower()
        for i in range(len(phrase)):
            if (phrase[i] in string.punctuation):
                holder+=[i]
        for i in holder:
            if ((i<(len(phrase)-1)) & (phrase[i]=="'") & (phrase[i+1]=="s")):
                flag_holder+=[False]
            elif ((i==(len(phrase)-1)) & (phrase[i]=="'")):
                flag_holder+=[False]
            else:
                flag_holder+=[True]
                punctuation_holder+=[i]
        #print(flag_holder)
        ne_phrase.set_punctuation_holder(punctuation_holder)
        if True in flag_holder:
            ne_phrase.set_feature(ne.has_intermediate_punctuation,True)
        else:
            ne_phrase.set_feature(ne.has_intermediate_punctuation,False)
        return ne_phrase


    # In[315]:

    def tense_check(self,ne_phrase):
        words=(((ne_phrase.phraseText.strip()).rstrip(string.punctuation)).lower()).split()
        verb_flag=False
        adverb_flag=False
        if (len(words)==1):
            if words[0].endswith("ing"):
                verb_flag=True
            if words[0].endswith("ly"):
                adverb_flag=True
        ne_phrase.set_feature(ne.ends_like_verb,verb_flag)
        ne_phrase.set_feature(ne.ends_like_adverb,adverb_flag)
        return ne_phrase


    # In[316]:

    def capitalization_change(self,ne_element):
        phrase=((ne_element.phraseText.lstrip(string.punctuation)).rstrip(string.punctuation)).strip()
        val=-1
        topic_indicator=False
        p1= re.compile(r'[A-Z]*\s*[A-Z]{4,}[^A-Za-z]*\s+[A-Za-z]+') #BREAKING: Toronto Raptors
        p2= re.compile(r'([A-Z]{1}[a-z]+)+[^A-Za-z]*\s+[A-Z]{4,}') #The DREAMIEST LAND
        match_lst1 = p1.findall(phrase)
        match_lst2 = p2.findall(phrase)
        if (match_lst1):
            if not phrase.isupper():
                p3=re.compile(r'[A-Z]*\s*[A-Z]{4,}[^A-Za-z]*\s+')
                val=list(p3.finditer(phrase))[-1].span()[1]
                if(":" in phrase):
                    topic_indicator=True
                ne_element.set_feature(ne.change_in_capitalization,val)
        elif (match_lst2):
            #print ("GOTIT2: "+phrase)
            p3=re.compile(r'([A-Z]{1}[a-z]+)+')
            val=list(p3.finditer(phrase))[-1].span()[1]
            ne_element.set_feature(ne.change_in_capitalization,val)
        else:
            ne_element.set_feature(ne.change_in_capitalization,val)
        ne_element.set_feature(ne.has_topic_indicator,topic_indicator)
        return ne_element


    # In[317]:

    def quoteProcess(self,unitQuoted, tweetWordList):
        candidateString=""
        retList=[]
        matches=[]
        final=[]

        for index in range(len(unitQuoted)-1):
            candidateString+=tweetWordList[unitQuoted[index]]+" "
        candidateString+=tweetWordList[unitQuoted[-1]]
        #print(candidateString)
        p= re.compile(r'[^\S]*(".*?")[^a-zA-Z0-9\s]*[\s]*')
        p1=re.compile(r'[^\s]+(".*?")[^\s]*')
        p2=re.compile(r'[^\s]*(".*?")[^\s]+')
        indices= (list(p.finditer(candidateString)))
        indices1= (list(p1.finditer(candidateString)))
        indices2= (list(p2.finditer(candidateString)))
        if ((len(indices)>0) & (len(indices1)==0)& (len(indices2)==0)):
            for index in indices:
                span= list(index.span())
                #print(span[0])
                matches+=[int(span[0]),int(span[1])]
            #print(matches)
            final+=[candidateString[0:matches[0]]]
            for i in range(len(matches)-1):
                final+=[(candidateString[matches[i]:matches[i+1]]).strip()]
            final+=[candidateString[matches[-1]:]]
            final=list(filter(lambda strin: strin!="",final))
            final=list(map(lambda strin: strin.strip(),final))
            for unit in final:
                lst=[]
                unitsplit=list(filter(lambda unitString: unitString!='',unit.split()))
                for splitunit in unitsplit:
                    lst+=[tweetWordList.index(splitunit)]
                retList+=[lst]
        else:
            retList+=[unitQuoted]
        #print("==>")
        return retList



    # In[318]:

    def trueEntity_process(self,tweetWordList_cappos,tweetWordList,q):
        
        #returns list with position of consecutively capitalized words
        output_unfiltered = self.consecutive_cap(tweetWordList_cappos,tweetWordList)
        #print(output_unfiltered)
        #splitting at quoted units
        output_quoteProcessed=[]
        for unitQuoted in output_unfiltered:
            out=self.quoteProcess(unitQuoted, tweetWordList)
            output_quoteProcessed+=out
        #print(output_quoteProcessed)
        #output_1= list(filter(lambda list_in: len(list_in)<7, output_quoteProcessed))
        output= list(filter(lambda list_in: list_in!=[0], output_quoteProcessed))

        #consecutive capitalized phrases 
        consecutive_cap_phrases1=list(map(lambda x: self.f(x,tweetWordList), output))

        consecutive_cap_phrases=list(filter(lambda candidate:(candidate.phraseText!="JUST_DIGIT_ERROR"),consecutive_cap_phrases1))

        
        #implement the punctuation clause
        ne_List_pc=self.flatten(list(map(lambda NE_phrase: self.punct_clause(NE_phrase), consecutive_cap_phrases)),[])
        

        #implement title detection
        ne_List_titleCheck= list(map(lambda element: self.title_check(element), ne_List_pc))
        
        #implement slang check and remove
        ne_List_slangCheck= list(filter(lambda element: not self.slang_remove(element), ne_List_titleCheck))
        
        #implement apostrophe, tense and punctuation marker with final number check
        ne_List_apostropeCheck= list(map(lambda element: self.apostrope_check(element), ne_List_slangCheck))
        ne_List_punctuationCheck= list(map(lambda element: self.punctuation_check(element), ne_List_apostropeCheck))
        ne_List_numCheck=list(filter(lambda candidate: not (candidate.phraseText.lstrip(string.punctuation).rstrip(string.punctuation).strip()).isdigit(), ne_List_punctuationCheck))
        ne_List_tenseCheck= list(map(lambda element: self.tense_check(element), ne_List_numCheck))
        
        #tracking sudden change in capitalization pattern
        ne_List_capPatCheck= list(map(lambda element: self.capitalization_change(element), ne_List_tenseCheck))
        
        #check on length
        ne_List_lengthCheck= list(filter(lambda element: element.length<7, ne_List_capPatCheck))
        combined=[]+list(cachedStopWords)+cachedTitles+prep_list+chat_word_list+article_list+day_list+month_list
        ne_List_badWordCheck= list(filter(lambda element:((element.phraseText.strip().lstrip('“‘’”')).rstrip('“‘’”').lower()) not in combined, ne_List_lengthCheck))
        ne_List_allCheck= list(filter(lambda element:(len((element.phraseText.strip().lstrip('“‘’”')).rstrip('“‘’”'))>1),ne_List_badWordCheck))
        q.put(ne_List_allCheck)
#return ne_List_allCheck


# In[319]:


'''This is the main module. I am not explicitly writing it as a function as I am not sure what argument you are 
passing.However you can call this whole cell as a function and it will call the rest of the functions in my module
to extract candidates and features
'''

'''#reads input from the database file and converts to a dataframe. You can change this part accordingly and
#directly convert argument tuple to the dataframe'''

#Inputs: Collection.csv 500Sample.csv 3.2KSample.csv eric_trump.csv

#df_out.to_csv('TweetBase500.csv')
#--------------------------------------PHASE I---------------------------------------------------


# In[ ]:

#--------------------------------------PHASE II---------------------------------------------------
'''set1 = set(['Melania','Trump'])
set2 = set(['Donald','Trump'])
set3 = set(['Jared','Kushner'])

m1 = MinHash(num_perm=200)
m2 = MinHash(num_perm=200)
m3 = MinHash(num_perm=200)
for d in set1:
    m1.update(d.encode('utf8'))

for d in set2:
    m2.update(d.encode('utf8'))
for d in set3:
    m3.update(d.encode('utf8'))

# Create LSH index
lsh = MinHashLSH(threshold=0.0, num_perm=200)
lsh.insert("m2", m2)
lsh.insert("m3", m3)
result = lsh.query(m1)
print("Approximate neighbours with Jaccard similarity", result)


candidates=["donald trump","melania trump", "obama","barack obama","barack"]
listofMinhash=[]
m=MinHash(num_perm=200)
candidate0=set(candidates[0].split())
for d in candidate0:
    m.update(d.encode('utf8'))
listofMinhash.append(m)
lsh = MinHashLSH(threshold=0.0, num_perm=200)
lsh.insert("m2", m2)
for candidate in candidates[1:]:'''
    


# In[ ]:

'''
print ("Shingling articles...")

# The current shingle ID value to assign to the next new shingle we 
# encounter. When a shingle gets added to the dictionary, we'll increment this
# value.
curShingleID = 0

# Create a dictionary of the articles, mapping the article identifier (e.g., 
# "t8470") to the list of shingle IDs that appear in the document.
candidatesAsShingleSets = {};
  
candidateNames = []

t0 = time.time()

totalShingles = 0

for k in range(0, len(sorted_NE_container.keys())):
    # Read all of the words (they are all on one line) and split them by white space.
    words = list(sorted_NE_container.keys())[k].split(" ")
    
    # Retrieve the article ID, which is the first word on the line.  
    candidateID = k
    
    # Maintain a list of all document IDs.  
    candidateNames.append(candidateID)
    
    
    # 'shinglesInDoc' will hold all of the unique shingle IDs present in the current document.
    #If a shingle ID occurs multiple times in the document,
    # it will only appear once in the set (this is a property of Python sets).
    shinglesInCandidate = set()
    
    # For each word in the document...
    for index in range(0, len(words)):
        
        # Construct the shingle text by combining three words together.
        shingle = words[index]
        # Hash the shingle to a 32-bit integer.
        #crc = binascii.crc32("")
        crc = binascii.crc32(bytes(shingle, encoding="UTF-8")) & (0xffffffff)

        # Add the hash value to the list of shingles for the current document. 
        # Note that set objects will only add the value to the set if the set 
        # doesn't already contain it. 
        shinglesInCandidate.add(crc)
    
    # Store the completed list of shingles for this document in the dictionary.
    #print(str(words)+": ")
    #for i in shinglesInCandidate:
     #   print('0x%08x' %i)
    candidatesAsShingleSets[candidateID] = shinglesInCandidate
    
    # Count the number of shingles across all documents.
    totalShingles = totalShingles + (len(words))


# Report how long shingling took.
print ('\nShingling ' + str(str(len(sorted_NE_container.keys()))) + ' candidates took %.2f sec.' % (time.time() - t0))
 
print ('\nAverage shingles per doc: %.2f' % (totalShingles / len(sorted_NE_container.keys())))
'''


# In[ ]:

'''
# =============================================================================
#                 Generate MinHash Signatures
# =============================================================================
numHashes=20
numCandidates=len(sorted_NE_container.keys())
# Time this step.
t0 = time.time()

print ('Generating random hash functions...')

# Record the maximum shingle ID that we assigned.
maxShingleID = 2**32-1
nextPrime = 4294967311


# Our random hash function will take the form of:
#   h(x) = (a*x + b) % c
# Where 'x' is the input value, 'a' and 'b' are random coefficients, and 'c' is
# a prime number just greater than maxShingleID.

# Generate a list of 'k' random coefficients for the random hash functions,
# while ensuring that the same value does not appear multiple times in the 
# list.
def pickRandomCoeffs(k):
    # Create a list of 'k' random values.
    randList = []
    
    while k > 0:
        # Get a random shingle ID.
        randIndex = random.randint(0, maxShingleID) 
        # Ensure that each random number is unique.
        while randIndex in randList:
            randIndex = random.randint(0, maxShingleID) 
        # Add the random number to the list.
        randList.append(randIndex)
        k = k - 1
    return randList

# For each of the 'numHashes' hash functions, generate a different coefficient 'a' and 'b'.   
coeffA = pickRandomCoeffs(numHashes)
coeffB = pickRandomCoeffs(numHashes)

print ('\nGenerating MinHash signatures for all candidates...')

# List of documents represented as signature vectors
signatures =np.ndarray(shape=(20, numCandidates))

# Rather than generating a random permutation of all possible shingles, 
# we'll just hash the IDs of the shingles that are *actually in the document*,
# then take the lowest resulting hash code value. This corresponds to the index 
# of the first shingle that you would have encountered in the random order.

# For each document...
for candidateID in candidateNames:
    
    # Get the shingle set for this document.
    shingleIDSet = candidatesAsShingleSets[candidateID]
  
    # The resulting minhash signature for this document. 
    signature = []
  
    # For each of the random hash functions...
    for i in range(0, numHashes):
        

        # For each of the shingles actually in the document, calculate its hash code
        # using hash function 'i'. 

        # Track the lowest hash ID seen. Initialize 'minHashCode' to be greater than
        # the maximum possible value output by the hash.
        minHashCode = nextPrime + 1

        # For each shingle in the document...
        for shingleID in shingleIDSet:
            # Evaluate the hash function.
            hashCode = (coeffA[i] * shingleID + coeffB[i]) % nextPrime

            # Track the lowest hash code seen.
            if hashCode < minHashCode:
                minHashCode = hashCode

        # Add the smallest hash code value as component number 'i' of the signature.
        signature.append(minHashCode)

    # Store the MinHash signature for this document.
    #signatures.append(signature)
    signatures[:,candidateID]=signature

    # Calculate the elapsed time (in seconds)
    elapsed = (time.time() - t0)
print(list(np.shape(signatures)))
print ("\nGenerating MinHash signatures took %.2fsec" % elapsed)


#print ('\nsignatures stored in a numpy array...')
  
# Creates a N x N matrix initialized to 0.

# Time this step.
t0 = time.time()

# For each of the test documents...
for i in range(10, 11):
#for i in range(0, numCandidates):
    print(list(sorted_NE_container.keys())[i]+": ",end="")
    # Get the MinHash signature for document i.
    signature1 = signatures[i]
    
    # For each of the other test documents...
    for j in range(0, numCandidates):
        if(j!=i):
            # Get the MinHash signature for document j.
            signature2 = signatures[j]

            count = 0
            # Count the number of positions in the minhash signature which are equal.
            for k in range(0, numHashes):
                count = count + (signature1[k] == signature2[k])

            # Record the percentage of positions which matched.    
            estJSim= (count / numHashes)
            #print(estJSim)
            if (estJSim>=0.5):
                print("=>"+list(sorted_NE_container.keys())[j]+", ",end="") 
    print()
# Calculate the elapsed time (in seconds)
elapsed = (time.time() - t0)
        
print ("\nComparing MinHash signatures took %.2fsec" % elapsed)'''


# In[ ]:

'''cap_phrases="Trump:Russia,Afgha"
words=re.split('[,:]', cap_phrases)
print(words)



candidateString='"BS'
p= re.compile(r'(".*?")[^\s]*[\s]*')
indices= (list( p.finditer(candidateString) ))
matches=[]
final=[]
if(indices):
    for index in indices:
        span= list(index.span())
        #print(span[0])
        matches+=[int(span[0]),int(span[1])]
    print(matches)
    final+=[candidateString[0:matches[0]]]
    for i in range(len(matches)-1):
        final+=[(candidateString[matches[i]:matches[i+1]]).strip()]
    final+=[candidateString[matches[-1]:]]
    final=list(filter(lambda strin: strin!="",final))
    final=list(map(lambda strin: strin.strip(),final))
    print(final)'''


# In[ ]:

#lst=[[2],[5]]
#type(lst)


# In[323]:

'''cap_phrases="Trotsky ALWAYS COULD,then he killed millions of racists:ONLY White People."
p1= re.compile(r'(?:[a-zA-Z0-9][^a-zA-Z0-9_\s]\s*)')
match_lst = p1.findall(cap_phrases)
print(match_lst)

print(cachedStopWords)'''


# In[ ]:




# In[ ]:



