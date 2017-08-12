#!/bin/bash


DATE=20141231
for (( i=0;i<31;i++)); do
    DATE=$(date +%Y%m%d -d "$DATE + 1 day")
    cat pagecounts-$DATE*\.gz >>$DATE\.gz
    rm -rf pagecounts-$DATE*\.gz
done

