 
from preprocessing import Preprocessing
import json

if __name__ == '__main__':
    
    myjob1 = Preprocessing()
    with myjob1.make_runner() as runner:
        runner.run()
        # returns a generator
        runner.cat_output()
            
         
