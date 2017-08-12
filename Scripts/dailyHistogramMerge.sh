#!/bin/bash

DATE=20071231
for (( i=0;i<31;i++)); do
    DATE=$(date +%Y%m%d -d "$DATE + 1 day")
    echo $DATE tarihli histogram oluşturuluyor.
   ~/hadoop-2.6.3/bin/./hadoop jar ~/Jars/HistogramSummarize-1.0-SNAPSHOT.jar edu.tou.HistogramSummarize 3 254 log_files/$DATE /dailyHistograms 
  
done

