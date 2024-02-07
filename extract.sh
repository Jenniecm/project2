#!/bin/bash

# Définir le lien vers le fichier à télécharger
url="https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz"

# Date et heure actuelles
current_date=$(date +'%Y-%m-%d')
current_time=$(date +'%H-%M-%S')

# Définir le nom du fichier téléchargé
filename="/home/ubuntu/Documents/food_data.csv.gz"

# Télécharger le fichier depuis le lien
wget "$url" -O "$filename"

# Vérifier si le téléchargement a réussi
if [ $? -eq 0 ]; then
    echo "Téléchargement réussi."
else
    echo "Erreur lors du téléchargement."
    exit 1
fi

# Décompresser le fichier gzip
gunzip "$filename"

# Vérifier si la décompression a réussi
if [ $? -eq 0 ]; then
    echo "Décompression réussie."
else
    echo "Erreur lors de la décompression."
    exit 1
fi

#suppression du fichier zip
rm "$filename"

# Chemin du fichier décompressé
decompressed_file="/home/ubuntu/Documents/food_data.csv"

# Copier le fichier décompressé dans HDFS
hdfs dfs -put -f "$decompressed_file" "/user/project/raw/"
if [ $? -eq 0 ]; then
    echo "Données copiées dans HDFS."
else
    echo "Erreur lors de la sauvegarde"
    exit 1
fi
