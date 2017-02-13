capitalized=0
start_of_sentence=1
abbreviation=2
all_capitalized=3
is_csl=4
title=5

class NE_candidate:
    """A simple NE_candidate class"""
    
    def __init__(self, phrase):
        
        length=0
        global capitalized
        global start_of_sentence
        global abbreviation
        global all_capitalized
        global is_csl
        global title
        '''other features:
                POS_isAdjective
                POS_endwithAdjective
                Title
        '''
        self.phraseText=phrase
        self.length=len(phrase.split())
        self.features = [None]*6
        return
    
    
    def set_feature(self, feature_index, feature_value):
        self.features[feature_index]= feature_value
        return
    
    def reset_length(self):
        self.length=len(self.phraseText.split())
        return
    
    def print_obj(self):
        print (self.phraseText+" "+str(self.length), end=" ")
        #print self.phraseText+" "+str(self.length),
        for feature in self.features:
            print (feature, end=" ")
        print ("")
        return


'''def main():
    my_obj = NE_candidate("sample",1)
    for i in range(5):
        my_obj.set_feature(i,True)
    my_obj.print_obj()
    print my_obj.features[capitalized]'''