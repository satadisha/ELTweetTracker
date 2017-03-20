capitalized=0
start_of_sentence=1
abbreviation=2
all_capitalized=3
is_csl=4
title=5
is_number=6
has_number=7
date_indicator=8

class NE_candidate:
    """A simple NE_candidate class"""
    
    def __init__(self, phrase, position):
        length=0
        global capitalized
        global start_of_sentence
        global abbreviation
        global all_capitalized
        global is_csl
        global title
        global is_number
        global has_number
        global date_indicator
        '''other features:
                POS_isAdjective
                POS_endwithAdjective
                Title
        '''
        self.phraseText=phrase
        self.position=position
        self.date_num_holder=[]
        self.length=len(phrase.split())
        self.features = [None]*9
        return
    
    
    def set_feature(self, feature_index, feature_value):
        self.features[feature_index]= feature_value
        return
    
    def reset_length(self):
        self.length=len(self.phraseText.split())
        return
    
    def print_obj(self):
        print (self.phraseText+" "+str(self.length)+" "+str(self.position)+" "+str(self.date_num_holder), end=" ")
        #print self.phraseText+" "+str(self.length),
        for feature in self.features:
            print (feature, end=" ")
        print ("")
        return



