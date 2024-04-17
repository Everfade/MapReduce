from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import re
import os
import sys
from collections import defaultdict

class Preprocessing(MRJob):
    """
    Needs to produce this information:
    A … number of reviews in c which contain w                  --> check
    B … number of reviews not in c which contain w              --> nope
    C … number of reviews in c without w                        --> check calc via = n[c] - A
    D … number of reviews not in c without w                    --> nope
    N … total number of reviews (can be omitted for ranking)    --> check direkt mit increment counter
    shortcuts:
    n … number of reviews of category   -> to calculate other information

    additional shortcuts: 
    A + B = N (direkt über N) -> dadurch auch B:   B = N - A
    A + C = number of reviews in this category (über n[cat]) -> dadurch auch C: C = n[cat] - A
    B + D = number of all reviews not in this category (über n[all] - n[cat]) -> dadurch auch D: D = n[all] -n[cat]
    C + D = number of reviews without w (über C + D von davor)

    Dann den shit berechnen:
    Formel:

            N*(AD-BC)
    -----------------------
    (A+B)*(A+C)*(B+D)*(C+D)
    """

    def mapper_preprocess(self, _, line):
        # load data
        data = json.loads(line)
        review_text = data['reviewText']
        category = data['category']
        self.increment_counter(group='total_reviews', counter='total_reviews', amount=1)        
        self.increment_counter(group='reviews_per_category', counter=category, amount=1)
        # split into words
        DELIMITERS = r"[\s\t\d\(\)\[\]\{\}\.\!\?,;:\+=\-_\"'`~#@&*%€$§\\/]+"
        words = re.split(DELIMITERS, review_text.lower())

        # filter out stopwords
        stopwords = self.read_stopwords()
        review_words = set() # need number of reviews per category which contain word, not number of word per category
        for word in words:
            if word not in stopwords and len(word) > 1 and word not in review_words:
                review_words.add(word)
                yield (category, word), 1

    def reducer_count_words(self, key, values):
        total = sum(values)
        category, word = key
        yield category, (word, total)

    def reducer_count_reviews_with_words(self, key, values):
        word, reviewcount_with_word_per_category = values
        reviewcount_with_word_total = sum(reviewcount_with_word_per_category)
        category = key
        yield (category, word), (reviewcount_with_word_per_category, reviewcount_with_word_total)
 
    """
    # number of categories which contain the word -> useless?
    def mapper_count_reviews_per_word(self, key, value):
        word = value[0]
        yield word, 1

    def reducer_sum_reviews_per_word(self, word, counts):
        yield word, sum(counts)
    """

    def read_stopwords(self):
        stopwords = set()
        dirname = os.path.dirname(__file__)

        stopwords = set()
        script_dir = os.path.dirname(os.path.realpath(__file__))
        stopwords_file = os.path.join(script_dir, 'stopwords.txt')       
        with open(stopwords_file, "r") as file:
         for line in file.readlines():
            stripped_line = line.strip()   
            stopwords.add(stripped_line)

        return stopwords
    

    def steps(self):
        return [
            MRStep(mapper=self.mapper_preprocess,
                   reducer=self.reducer_count_words)
        ]
if __name__ == '__main__':
    Preprocessing.run()

#Für seprates testen python preprocessing.py reviews_devset.json > output.txt

