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
        DELIMITERS = r"[\s\t\d\(\)\[\]\{\}\.\!\?,;:\+=\-_\"'`~#@&*%€$§\\/]+"
        words = re.split(DELIMITERS, review_text.lower())

        # stopwards
        stopwords = self.read_stopwords()
 
        for word in words:
            if word not in stopwords and len(word) > 1:
                yield (category, word), 1

    def reducer_count_words(self, key, values):
        total = sum(values)
        category, word = key
        yield category, (word, total)

 
    def read_stopwords(self):
        stopwords = set()
        dirname = os.path.dirname(__file__)
       # filename = os.path.join(dirname, 'stopwords.txt')
       #path :)
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

