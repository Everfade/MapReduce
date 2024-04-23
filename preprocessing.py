from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import re
import os

class Preprocessing(MRJob):
    """
    Needs to produce this information:
    A … number of reviews in c which contain w
    B … number of reviews not in c which contain w
    C … number of reviews in c without w
    D … number of reviews not in c without w

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
        yield word, (category, total)

    def reducer_count_words_per_review(self, key, values):
        cats = []
        counts = []
        count_of_cat = {}
        
        cat_set = set()
        for item in values:
            cats.append(item[0])
            cat_set.add(item[0])
            counts.append(item[1])

        for c in cat_set:
            for i, cat in enumerate(cats):
                if cat == c:
                    if count_of_cat.get(cat) is None:
                        count_of_cat[cat] = counts[i]
                    else: 
                        count_of_cat[cat] = count_of_cat[cat] + counts[i]                        
            
            yield (([key, c]), ([count_of_cat[c], sum(counts)]))

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
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_count_words_per_review)
        ]
if __name__ == '__main__':
    Preprocessing.run()


#Für seprates testen python preprocessing.py reviews_devset.json > values_for_chi2.txt

