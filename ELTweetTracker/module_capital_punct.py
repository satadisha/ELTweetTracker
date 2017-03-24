import sys
import re
import string
from pandas import read_csv
from itertools import groupby
from operator import itemgetter
from collections import Iterable
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.corpus import stopwords
import NE_candidate_module as ne
import Mention
import threading, queue

cachedStopWords = set(stopwords.words("english"))
cachedStopWords.update(("and","make","oh","via","i","i'll","would","should","shall","let's","let","well","just","someone","theres","somebody","didn't","i've","they're","we're","we'll","we've","they've","they'd","they'll","again","you're","thats","that's","i'm","a","so","except","arn't","aren't","arent","this","when","it","it's","he's","she's","she'd","he'd","he'll","she'll","many","can't","even","cant","yes","no","these","to","may","maybe","<hashtag>","<hashtag>.","ever","never","there's","there’s"))
cachedTitles = ["Mr.","Mr","Mrs.","Mrs","Miss","Ms","Sen."]
prep_list=["in","at","of","on","and","by"] #includes common conjunction as well
article_list=["a","an","the"]
day_list=["sunday","monday","tuesday","wednesday","thursday","friday","saturday","mon","tues","wed","thurs","fri","sat","sun"]
month_list=["january","february","march","april","may","june","july","august","september","october","november","december","jan","feb","mar","apr","may","jun","jul","aug","sep","oct","nov","dec"]
chat_word_list=["congrats","whoa","hey","hi","damn","lol","omg","congratulations","fuck","wtf","wtaf","xoxo","rofl","imo","wow","fck","haha","hehe","hoho"]
NE_container={}

def insert_dict(key):
    #print(key)
    if key in NE_container:
        NE_container[key] += 1
    else:
        NE_container[key] = 1
    return
def printList(mylist):
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

def flatten(mylist, outlist,ignore_types=(str, bytes, ne.NE_candidate)):
    
    if mylist !=[]:
        for item in mylist:
            #print not isinstance(item, ne.NE_candidate)
            if isinstance(item, list) and not isinstance(item, ignore_types):
                flatten(item, outlist)
            else:
                if isinstance(item,ne.NE_candidate):
                    item.phraseText=item.phraseText.strip(' \t\n\r')
                    item.reset_length()
                else:
                    item=item.strip(' \t\n\r')
                outlist.append(item)
    return outlist

def consecutive_cap(tweetWordList_cappos,tweetWordList):
    output=[]
    #identifies consecutive numbers in the sequence
    for k, g in groupby(enumerate(tweetWordList_cappos), lambda element: element[0]-element[1]):
        output.append(list(map(itemgetter(1), g)))
    count=0
    if output:        
        final_output=[output[0]]
        for first, second in (zip(output,output[1:])):
            if ((second[0]-first[-1])==2) & (tweetWordList[first[-1]+1].lower() in prep_list):
                (final_output[-1]).extend([first[-1]+1]+second)
            elif((second[0]-first[-1])==3) & (tweetWordList[first[-1]+1].lower() in prep_list)& (tweetWordList[first[-1]+2].lower() in article_list):
                (final_output[-1]).extend([first[-1]+1]+[first[-1]+2]+second)
            else:
                final_output.append(second)
                #merge_positions.append(False)
    else:
        final_output=[]
    
    return final_output

#basically splitting the original NE_candidate text and building individual object from each text snippet
def build_custom_NE(phrase,pos,prototype,feature_index,feature_value):
    #print("Enters")
    position=pos
    custom_NE= ne.NE_candidate(phrase,position)
    for i in range(14):
        custom_NE.set_feature(i,prototype.features[i])
    custom_NE.set_feature(feature_index,feature_value)
    if (feature_index== ne.is_csl) & (feature_value== True):
        custom_NE.set_feature(ne.start_of_sentence, False)
    custom_NE=entity_info_check(custom_NE)
    return custom_NE

def abbrv_algo(ne_element):
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
                print ("3. Found abbreviation!!: "+phrase)
            
    #element= ne.NE_candidate(phrase.strip())
    ne_element.phraseText=phrase
    ne_element.reset_length()
    ne_element.set_feature(ne.abbreviation, abbreviation_flag)
    return ne_element

def punct_clause(NE_phrase_in):
    
    NE_phrases=entity_info_check(NE_phrase_in)
    cap_phrases=NE_phrases.phraseText.strip()
    #print (cap_phrases)
    if (re.compile(r'[^a-zA-Z0-9_\s]')).findall(cap_phrases):
        #case of intermediate punctuations: handles abbreviations
        p1= re.compile(r'(?:[a-zA-Z0-9][^a-zA-Z0-9_\s]\s*)')
        match_lst = p1.findall(cap_phrases)
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
    start_of_sentence_fix=NE_phrases.features[ne.start_of_sentence]
    wordlst=list(filter(lambda word: word!="", re.split('[,]', cap_phrases)))
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
        final_lst= list(map(lambda element:build_custom_NE(str(element[0]),element[1:],NE_phrases,ne.is_csl,True), lst_nsw))
        final_lst[0].set_feature(ne.start_of_sentence, NE_phrases.features[ne.start_of_sentence])
    else:
        NE_phrases.set_feature(ne.is_csl,False)
        final_lst=[NE_phrases]
    
    #check abbreviation
    final_lst= list(map(lambda phrase: abbrv_algo(phrase), final_lst))

    
    #print(lst)
    return final_lst

#%%timeit -o
def f(x,tweetWordList):

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


def capCheck(word):
    combined_list=[]+list(cachedStopWords)+prep_list+chat_word_list
    if word.startswith('@'):
        return False
    elif "<Hashtag" in word:
        return False
    elif ((word.lstrip(string.punctuation)).rstrip(string.punctuation)).lower() in combined_list:
        if(word!="The"):
            return False
        else:
            return True
    elif word[0].isdigit():
        return True
    else:
        p=re.compile(r'^[\W]*[A-Z]')
        l= p.match(word)
        if l:
            return True
        else:
            return False


def title_check(ne_phrase):
    title_flag=False
    words=ne_phrase.phraseText.split()
    for word in words:
        if word in cachedTitles:
            title_flag= True
            break
    ne_phrase.set_feature(ne.title,title_flag)
    return ne_phrase


def entity_info_check(ne_phrase):
    flag1=False #has number
    flag3=False
    flag_ind=[] #is number
    month_ind=[]
    date_num_holder=[]
    words=ne_phrase.phraseText.split()
    for word in words:
        word=(word.strip()).rstrip(string.punctuation).lower()
        if not word.isalpha():
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


#removing commonly used expletives, enunciated chat words and other common words (like days of the week, common expressions)
def slang_remove(ne_phrase):
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


def apostrope_check(ne_phrase):
    apostrophe="'s"
    phrase=(ne_phrase.phraseText.strip()).rstrip(string.punctuation).lower()
    if apostrophe in phrase:
        if phrase.endswith(apostrophe):
            ne_phrase.set_feature(ne.is_apostrophed,0)
        else:
            #print(phrase.find(apostrophe))
            ne_phrase.set_feature(ne.is_apostrophed,phrase.find(apostrophe))
    else:
        ne_phrase.set_feature(ne.is_apostrophed,-1)
    return ne_phrase


def punctuation_check(ne_phrase):
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


def tense_check(ne_phrase):
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


def capitalization_change(ne_element):
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


def trueEntity_process(tweetWordList_cappos,tweetWordList,q):
    
    #returns list with position of consecutively capitalized words
    output = consecutive_cap(tweetWordList_cappos,tweetWordList)

    #consecutive capitalized phrases 
    consecutive_cap_phrases1=list(map(lambda x: f(x,tweetWordList), output))

    consecutive_cap_phrases=list(filter(lambda candidate:(candidate.phraseText!="JUST_DIGIT_ERROR"),consecutive_cap_phrases1))

    
    #implement the punctuation clause
    ne_List_pc=flatten(list(map(lambda NE_phrase: punct_clause(NE_phrase), consecutive_cap_phrases)),[])
    

    #implement title detection
    ne_List_titleCheck= list(map(lambda element: title_check(element), ne_List_pc))
    
    #implement slang check and remove
    ne_List_slangCheck= list(filter(lambda element: not slang_remove(element), ne_List_titleCheck))
    
    #implement apostrophe, tense and punctuation marker with final number check
    ne_List_apostropeCheck= list(map(lambda element: apostrope_check(element), ne_List_slangCheck))
    ne_List_punctuationCheck= list(map(lambda element: punctuation_check(element), ne_List_apostropeCheck))
    ne_List_numCheck=list(filter(lambda candidate: not (candidate.phraseText.lstrip(string.punctuation).rstrip(string.punctuation).strip()).isdigit(), ne_List_punctuationCheck))
    ne_List_tenseCheck= list(map(lambda element: tense_check(element), ne_List_numCheck))
    
    #tracking sudden change in capitalization pattern
    ne_List_allCheck= list(map(lambda element: capitalization_change(element), ne_List_tenseCheck))
    
    q.put(ne_List_allCheck)
    #return ne_List_allCheck


'''This is the main module. I am not explicitly writing it as a function as I am not sure what argument you are 
passing.However you can call this whole cell as a function and it will call the rest of the functions in my module
to extract candidates and features
'''

'''#reads input from the database file and converts to a dataframe. You can change this part accordingly and
#directly convert argument tuple to the dataframe'''

#Collection.csv
df = read_csv('500Sample.csv', index_col='ID', header=0, encoding='utf-8')
#print (df.columns.values.tolist())

#%%timeit -o
#module_capital_punct.main:
'''I am running this for 100 iterations for testing purposes. Of course you no longer need this for loop as you are
#running one tuple at a time'''

count=0
ne_count=0
userMention_count=0
token_count=0

NE_list_phase1=[]
UserMention_list=[]

ME_EXTR=Mention.Mention_Extraction()
#--------------------------------------PHASE I---------------------------------------------------
for index, row in df.iterrows():
    
    if count<500:
        #tweetText=unicode(row['Tweets']).encode('utf-8')
        userType=str(row['User Type'])
        tweetText=str(row['Tweet Text'])
        #tweetText=row['Tweets']
        #print(str(index)+". "+userType+":=>\n"+tweetText)
        #capitalization module
        #if all words are capitalized:
        if tweetText.isupper():
            #print "All caps module"
            print ("All caps module\n")
        elif tweetText.islower():
            #print "All caps module"
            print ("All lower module\n")
        else:
            ne_List_final=[]
            userMention_List_final=[]
            #pre-modification: returns word list split at whitespaces; retains punctuation
            tweetSentences=list(filter (lambda sentence: len(sentence)>0, tweetText.split('\n')))
            tweetSentenceList=flatten(list(map(lambda sentText: sent_tokenize(sentText.lstrip().rstrip()),tweetSentences)),[])
            
            #printList(tweetSentenceList)
            for sentence in tweetSentenceList:
                tweetWordList= sentence.split()
                
                token_count+=len(tweetWordList)
                #print (tweetWordList)
                #returns position of words that are capitalized
                tweetWordList_cappos = list(map(lambda element : element[0], filter(lambda element : capCheck(element[1]), enumerate(tweetWordList))))
                
                #returns list of @userMentions
                userMentionswPunct=list(filter(lambda phrase: phrase.startswith('@'), tweetWordList))
                userMentions=list(map(lambda mention: mention.rstrip(string.punctuation), userMentionswPunct))
                
                #non @usermentions are processed in this function to find non @, non hashtag Entities
                q = queue.Queue()
                threading.Thread(target=trueEntity_process, args=(tweetWordList_cappos,tweetWordList,q)).start()
                ne_List_allCheck= q.get()
                #ne_List_allCheck= trueEntity_process(tweetWordList_cappos,tweetWordList)
                
                #function to process and store @ user mentions
                #dbg
                
                ne_count+=len(ne_List_allCheck)
                userMention_count+=len(userMentions)
                
                ne_List_final+=ne_List_allCheck
                userMention_List_final+=userMentions
                threading.Thread(target=ME_EXTR.ComputeAll, args=(userMention_List_final,)).start()
##            #printList(ne_List_final)
##            if(userMention_List_final):
##                print(userMention_List_final)
##                #ME_EXTR.ComputeAll(userMention_List_final)
##                threading.Thread(target=ME_EXTR.ComputeAll, args=(userMention_List_final,)).start()
##
##
##            NE_list_phase1+=ne_List_final
##            UserMention_list+=userMention_List_final
##
##            print ("\n")
##        count+=1
##    else:
##        break
##print("Total number of tokens processed: "+str(token_count))
##print ("Total number of NEs extracted: "+str(ne_count))
##ME_EXTR.PrintDictionary()

                
            #insert_dict ((mention.phraseText.lstrip(string.punctuation)).rstrip(string.punctuation).strip()).lower()
            for candidate in ne_List_final:
                insert_dict ((((candidate.phraseText.lstrip(string.punctuation)).rstrip(string.punctuation)).rstrip("'s").strip()).lower())
            #printList(ne_List_final)
            '''if(userMention_List_final):
                print(userMention_List_final)
            
            NE_list_phase1+=ne_List_final
            UserMention_list+=userMention_List_final
            print ("\n")'''
        count+=1
    else:
        break
sorted_d = [(k,v) for v,k in sorted([(v,k) for k,v in NE_container.items()],reverse=True)]
for key, value in sorted_d:
    #print("=>")
    print (key+" : "+str(value))




print("Total number of tokens processed: "+str(token_count))
print ("Total number of NEs extracted: "+str(ne_count))
ME_EXTR.PrintDictionary()
print("**********************************************************")
ME_EXTR.EditDistance(sorted_d)
#--------------------------------------PHASE I---------------------------------------------------
