#!/bin/bash

database_name="indigg_assignment"

# Check if the database already exists
aws glue get-database --name "$database_name" >/dev/null 2>&1
if [[ $? -eq 0 ]]; then
  echo "Database '$database_name' already exists. Skipping creation."
else
  # Create the database
  aws glue create-database --database-input '{ "Name": "'"$indigg_assignment"'" }'
  echo "Database '$database_name' created."
fi

# Create the crawlers
aws glue create-crawler --name steam_video_game_crawler --database-name indigg_assignment --role aws_glue_role --targets S3Targets={Path='s3://aishwary-test-bucket/indigg-assignment-bucket/input/steam-200k.csv'}
aws glue create-crawler --name vgsales_crawler --database-name indigg_assignment --role aws_glue_role --targets S3Targets={Path='s3://aishwary-test-bucket/indigg-assignment-bucket/input/vgsales.csv'}
aws glue start-crawler --name steam_video_game_crawler
aws glue start-crawler --name vgsales_crawler

# Check if the crawlers are ready

while true
do
    status_steam=$(aws glue get-crawler --name steam_video_game_crawler --query 'Crawler.State' --output text)
    status_sales=$(aws glue get-crawler --name vgsales_crawler --query 'Crawler.State' --output text)
    echo "steam_video_game_crawler status: $status_steam"
    echo "vgsales_crawler status: $status_sales"

    if [[ "$status_steam" == "READY" && "$status_sales" == "READY" ]]; then
        break
    fi
    echo "Waiting for 10 seconds"
    sleep 10
done
echo "Glue crawler ran successfully"