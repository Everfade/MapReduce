from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import re
import os
from collections import defaultdict



script_dir = os.path.dirname(os.path.realpath(__file__))
counters = os.path.join(script_dir, 'category_counters.txt')       
reviews_per_category = {}

with open(counters, "r") as file:
    for line in file.readlines():
        y = line.replace("\"", "")
        x = y.split()
        category = x[0]
        counter = int(x[1])
        reviews_per_category[category] = counter
total_reviews = sum(reviews_per_category.values())


class CalculateChi(MRJob):
    """
    A … number of reviews in c which contain w                  --> check;
    B … number of reviews not in c which contain w              --> nope; need to calc
    C … number of reviews in c without w                        --> nope; calc via = n[c] - A
    D … number of reviews not in c without w                    --> nope; need to calc
    N … total number of reviews (can be omitted for ranking)    --> nope; add up category counters
    shortcuts:
    n … number of reviews of category   -> to calculate other information

    additional shortcuts: 
    A + B = N (direkt über N) -> dadurch auch B:   B = N - A BULLSHIT
    A + C = n[cat] number of reviews in this category (über n[cat]) -> dadurch auch C: C = n[cat] - A
    B + D = number of all reviews not in this category (über N - n[cat]) -> dadurch auch D: D = N -n[cat] - B
    C + D = number of reviews without w (über C + D von davor)

    brauche number of reviews which contain the word in total = X
    dann ist B = X - A

    Dann den shit berechnen:
    Formel:

            N*(AD-BC)^2
    -----------------------
    (A+B)*(A+C)*(B+D)*(C+D)
    """

    def mapper_calculate_chi2(self, _, line):
        
        line = line.replace("\"", "").replace("[", "").replace("]", "").replace(",", "").split()
        word = line[0]
        category = line[1]
        A = int(line[2])
        X = int(line[3])

        N = total_reviews
        n = reviews_per_category

        # Calculate variables
        B = X - A
        C = n[category] - A
        D = N - n[category] - B

        # Calculate Chi2
        chi2 = N*(A*D - B*C) ** 2 / ((A + B) * (C + D) * (A + C) * (B + D))
    
        yield category, (word, chi2)

    def steps(self):
        return [
            MRStep(mapper=self.mapper_calculate_chi2)
        ]
if __name__ == '__main__':
    CalculateChi.run()

#Für seprates testen python calculate_chi.py values_for_chi2.txt > output.txt
