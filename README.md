Diese Reihenfolge

1: python preprocessing.py reviews_devset.json > values_for_chi2.txt
2: python reviewcounter.py reviews_devset.json > category_counter.txt
3: python calculate_chi.py values_for_chi2.txt > output.txt
