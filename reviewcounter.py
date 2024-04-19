from mrjob.job import MRJob
import json
from mrjob.job import MRJob
from mrjob.step import MRStep
import json

class ReviewCountPerCategory(MRJob):
    """
    Needs to produce this information:
    N … total number of reviews (can be omitted for ranking)
    -> does it indirectly -> just sum up all category reviewcounts (we need individual category reviewcounts to calculate C and D)
    Formel:

            N*(AD-BC)
    -----------------------
    (A+B)*(A+C)*(B+D)*(C+D)
    """
    def mapper(self, _, line):
        data = json.loads(line)
        review_text = data['reviewText']
        category = data['category']
        yield category, 1

    def reducer(self, category, counts):
        total_reviews = sum(counts)
        yield (category, total_reviews)

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]
    
if __name__ == '__main__':
    ReviewCountPerCategory.run()


#Für seprates testen python reviewcounter.py reviews_devset.json > category_counter.txt