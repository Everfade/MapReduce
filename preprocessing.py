from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import re
import os
class Preprocessing(MRJob):

    def mapper_preprocess(self, _, line):
        # daATA LAODING
        data = json.loads(line)
        review_text = data['reviewText']
        category = data['category']
        WORD_RE = re.compile(r"[\s\t\d\(\)\[\]\{\}\.\!\?,;:\+=\-_\"'`~#@&*%€$§\\/]+")
        words = WORD_RE.split(review_text.lower())

        # stopwards
        stopwords = self.read_stopwords()
        words = [w for w in words if w not in stopwords and len(w) > 1]
        for word in words:
            yield (category, word), 1

    def reducer_count_words(self, key, values):
        total = sum(values)
        category, word = key
        yield category, (word, total)

    def reducer_chi_square(self, category, word_counts):
        # Not sure ob das hierher gehört
        pass
 
    def read_stopwords(self):
        stopwords = set()
        dirname = os.path.dirname(__file__)
       # filename = os.path.join(dirname, 'stopwords.txt')
       #path :)
        with open("D:\Tu\Master\DataIntensive\Exercise1\stopwords.txt", "r") as file:
         for line in file.readlines():
            stripped_line = line.strip()   
            stopwords.add(stripped_line)

        return stopwords
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_preprocess,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_chi_square)
        ]
if __name__ == '__main__':
    Preprocessing.run()

#Für seprates testen python preprocessing.py reviews_devset.json > output.txt

