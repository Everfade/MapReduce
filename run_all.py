import os
import time

start_time = time.time()

# python preprocessing.py reviews_devset.json > values_for_chi2.txt
# python reviewcounter.py reviews_devset.json > category_counters.txt
# python calculate_chi.py values_for_chi2.txt > output.txt

start_time = time.time()
preprocessing = "python preprocessing.py --hadoop-streaming-jar /usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.5.jar -r \
    hadoop hdfs:///user/dic24_shared/amazon-reviews/full/reviews_devset.json > values_for_chi2.txt"

review_counter = "python reviewcounter.py --hadoop-streaming-jar /usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.5.jar -r \
    hadoop hdfs:///user/dic24_shared/amazon-reviews/full/reviews_devset.json > category_counters.txt"

calculate_chi = "python calculate_chi.py --hadoop-streaming-jar /usr/lib/hadoop/tools/lib/hadoop-streaming-3.3.5.jar -r \
    values_for_chi2 > output.txt"

exit_code = os.system(preprocessing)
print("--- %s minutes ---" % ((time.time() - start_time)/60))
if exit_code == 0:
    print("Command executed successfully")
else:
    print("Command execution failed with code", exit_code)