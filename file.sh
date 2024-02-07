User
#!/bin/bash

wget -O ~/Downloads/$1.csv.gz https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz

gunzip ~/Downloads/$1.csv.gz

mv ~/Downloads/$1.csv ~/workspace/data/off_raw

hdfs dfs -put ~/workspace/data/off_raw/$1.csv /user/project/raw