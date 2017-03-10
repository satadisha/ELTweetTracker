import pandas as pd
import sys
import matplotlib.pyplot as plt
from numpy.random import normal
from collections import Counter


class Tweet:
    def __init__(self,extr_easy,tag):
        self.extr_easy=extr_easy
        self.tag=tag




#reading
df = pd.read_csv('final500.csv', skipinitialspace=True)

normal_user_holder=[]
journalist_holder=[]
company_holder=[]

for index, row in df.iterrows():
    usertype=str(row['User Type']).encode('utf-8').decode('utf-8')
    tag=str(row['Tag(Grammar)_Satadisha']).encode('utf-8').decode('utf-8')
    extr_easy=str(row['ExtractionEasiness_Satadisha']).encode('utf-8').decode('utf-8')
    if("," in tag):
        tag=tag.split(",")
        extr_easy=extr_easy.split(",")
    tweet1=Tweet(extr_easy,tag)
    journalist_holder.append(tweet1)


#testing
tweet_tag_holder=[]
tweet_easy_holder=[]

for tweet in journalist_holder:
    #print(type(tweet.tag))
    if type(tweet.tag) is list:
        for tags in tweet.tag:
            tweet_tag_holder.append(tags)

        for easiness in tweet.extr_easy:
            tweet_easy_holder.append(easiness)        
    else:

        tweet_tag_holder.append(str(tweet.tag))
        tweet_easy_holder.append(str(tweet.extr_easy))
#testing 

BE=0
BD=0
ME=0
MD=0
MsE=0
MsD=0
GE=0
GD=0



merger_holder=[]


print(len(tweet_tag_holder), len(tweet_easy_holder))
for idx,val in enumerate(tweet_tag_holder):
    if(idx>700):
        break
    merger_holder.append(tweet_tag_holder[idx]+tweet_easy_holder[idx])





for tag in merger_holder:
    print(tag)
    if("BE" in tag):
        BE=BE+1
    if("BD" in tag):
        BD=BD+1
    if("ME" in tag):
        ME=ME+1
    if("MD" in tag):
        MD=MD+1
    if("M*E" in tag):
        MsE=MsE+1
    if("M*D" in tag):
        MsD=MsD+1
    if("GE" in tag):
        GE=GE+1
    if("GD" in tag):
        GD=GD+1

data=[[ BE  ,BD,"B"],  #random
       [ ME,  MD,"M"], 
       [ MsE,  MsD,"M*"],
        [ GE, GD,"G"]
       ] #random




letter_counts = Counter(merger_holder)
print(letter_counts)
df = pd.DataFrame(data, columns=['E', 'D','Tag Types'])
df=df.set_index('Tag Types')
print(df)

#df = pd.DataFrame.from_dict(letter_counts, orient='index')
ax=df.plot(kind='bar')
fig = ax.get_figure()
fig.savefig('easy.png')
