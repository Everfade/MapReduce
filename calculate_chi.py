from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import re
import os
from collections import defaultdict

"""
stopwords = set()
dirname = os.path.dirname(__file__)

stopwords = set()
script_dir = os.path.dirname(os.path.realpath(__file__))
output = os.path.join(script_dir, 'output.txt')       

with open(output, "r") as file:
    for line in file.readlines():
        print(line.replace("\"", "").replace("[", "").replace("]", "").replace(",", ""))
        y = line.replace("\"", "").replace("[", "").replace("]", "").replace(",", "")
        print(y.split())
        x = y.split()
        category = x[0]
        word = x[1]
        count = x[2]
        print(f"cat: {category}, word: {word}, count: {type(int(count))}")
        break
"""

# kategorie, wort, (nt) wortanzahl über alle kategorien, (ntc) wortanzahl dieses wortes dieser kategorie

class CalculateChi(MRJob):
    """FUNKT NICHT -> muss noch überarbeiten"""

    def mapper_calculate_chi_square(self, _, line):
        
        y = line.replace("\"", "").replace("[", "").replace("]", "").replace(",", "")
        x = y.split()
        category = x[0]
        word = x[1]
        count = int(x[2])
        word_count_par = []

        # Collect word counts for each category
        category_word_counts = defaultdict(int)
        total_word_counts = defaultdict(int)

        for word, count in word_count_pairs:
            category_word_counts[word] += count
            total_word_counts[category] += count

        # Calculate chi-square values

        """
        A … number of reviews in c which contain w
        B … number of reviews not in c which contain t
        C … number of reviews in c without t
        D … number of reviews not in c without t
        N … total number of reviews (can be omitted for ranking)
        """
        chi_square_values = {}
        for word, count in category_word_counts.items():
            A = count  # wie oft kommt dieses wort in kategories vor -> HAB ICH
            B = total_word_counts[category] - count  # wortanzahl dieser kategorie (minus momentanem wort) -> HAB ICH NICHT
            C = sum(total_word_counts.values()) - total_word_counts[category]  # wie oft das wort in allen anderen kategorien zusammen vorkommt -> HAB ICH NICHT
            D = sum(total_word_counts.values()) - A - B - C  # Anzahl aller anderen Wörter in allen anderen Kategorien -> HAB ICH NICHT

            chi_square = (A*D - B*C) ** 2 / ((A + B) * (C + D) * (A + C) * (B + D))
            chi_square_values[word] = chi_square

        yield category, word, chi_square_values

    def steps(self):
        return [
            MRStep(mapper=self.mapper_calculate_chi_square)
        ]
if __name__ == '__main__':
    CalculateChi.run()

#Für seprates testen python calculate_chi.py output.txt > final_output.txt

