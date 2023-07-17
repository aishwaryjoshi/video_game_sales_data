#!/bin/bash


file_paths=(
    "steam-video-games.zip"
    "videogamesales.zip"
    "steam-200k.csv"
    "vgsales.csv"
)

for file_path in "${file_paths[@]}"
do
    if [ -f "$file_path" ]; then
        echo "Deleting $file_path..."
        rm "$file_path"
        echo "File deleted."
    else
        echo "File $file_path not found. Will download"
    fi
done
kaggle datasets download -d gregorut/videogamesales
kaggle datasets download -d tamber/steam-video-games
unzip steam-video-games.zip
unzip videogamesales.zip
aws s3 cp steam-200k.csv s3://aishwary-test-bucket/indigg-assignment-bucket/input/steam-200k.csv
aws s3 cp vgsales.csv s3://aishwary-test-bucket/indigg-assignment-bucket/input/vgsales.csv

for file_path in "${file_paths[@]}"
do
    if [ -f "$file_path" ]; then
        echo "Deleting $file_path..."
        rm "$file_path"
        echo "File deleted."
    else
        echo "File $file_path not found."
    fi
done 