import pandas as pd
fields = ['Tweets']


class Tweet:

    def __init__(self):
        self.Nef2s=[]

    def setId(self,id):
        self.id=id  

    def setText(self,text):
        self.text=text

    def addNeF2s(self,nef2):
        self.Nef2s.append(nef2)





with open('latest_output.txt', 'r',encoding="utf8") as myfile:
    data=myfile.read()


import re
x = re.findall(r'\n\n(.*?)\[',data,re.DOTALL)

tweet_holder=[]

counter =0


for element in x:
    parts = element.split('\n')
    tweet1= Tweet()
    tweet1.setText(parts[0])
    
    tweet_holder.append(tweet1)
    counter=counter+1



print(counter)    
#for extracting NF2s
y = re.findall(r'\[(.*?)\]',data,re.DOTALL)
counter=0

word_holder=[]
#print(x)

for element in y:
    parts = element.split('\n')
    #parts_string=''.join(parts)
    for part in parts:
        tweet_holder[counter].addNeF2s(part)
    counter=counter+1

for tweet in tweet_holder:
    del tweet.Nef2s[-1]
    del tweet.Nef2s[0]


Nef2TweetHolder=[]
c=0
for tweet in tweet_holder:
    for index,elem in enumerate(tweet.Nef2s):
        part2= re.split('(\d+)',elem)
        #print(part2[2])
        x = re.findall(r'([A-Z])\w+',part2[2],re.DOTALL)
        tweet.Nef2s[index]=part2[0]
        c=c+1
        #print(x[2])
        #print(c)
        #print(tweet.text)
        if x[2] is "T":
            Nef2TweetHolder.append(tweet)



#for tweet in Nef2TweetHolder:
    #print(tweet.text)
    #print(tweet.Nef2s)
    #print(tweet.id)
print(len(Nef2TweetHolder))



df = pd.read_csv('satadishaFinalNef2ManualAnnotated.csv',encoding = "iso-8859-1", skipinitialspace=True, usecols=fields,)

listsss=[]
listsss=df.Tweets.tolist()
add_list=[]

counter=0        
for sentence in listsss:
    sentence=sentence.replace("</NEf2>","")
    sentence=sentence.replace("</NE>","")
    tweet2=Tweet()
    tweet2.setText(sentence)

    print(sentence)
    add_list.append(tweet2)
    counter=counter+1
    #print(counter)

counter=0

for element in listsss:
    x = re.findall(r'</NEf2>(.*?)</NEf2>',element,re.DOTALL)
    #print(x)
    for el2 in x:
        #if counter<85:
        add_list[counter].addNeF2s(el2)
    counter=counter+1

        #print(counter)

        
#checker=[x for x in mert if x]

c_total=0

for tweet_pc in Nef2TweetHolder:
    for tweet_org in add_list:
        if tweet_pc.text in tweet_org.text:
            print("burdayim")
            print(tweet_pc.Nef2s)
            print(tweet_org.Nef2s)
            for nf2_pc in tweet_pc.Nef2s:
                for nf2_org in tweet_org.Nef2s:
                    #print (nf2_pc)
                    #print(nf2_org)
                    if nf2_org in nf2_pc:
                        print('hello')
                        c_total=c_total+1

print(c_total)


