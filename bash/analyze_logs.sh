#!/bin/bash

count_logs=0

while IFS= read -r line; do 
    count_logs=$((count_logs + 1))     
done < access.log

if [ -f "report.txt" ]; then
    > report.txt
else
    touch report.txt
fi

echo "Отчет о логе веб-сервера" >> report.txt
echo "========================" >> report.txt
echo "Общее количество запросов:" $count_logs >> report.txt
echo "Количество уникальных IP-адресов:" $(awk '{ips[$1]++} END {print length(ips)}' access.log) >> report.txt
echo "" >> report.txt
echo "Количество запросов по методам:" >> report.txt
awk '{count[$6]++} END {for (item in count) print substr(item, 2), count[item]}' access.log >> report.txt
echo "" >> report.txt
echo "Самый популярный URL:" >> report.txt
awk '{count[$7]++} END {for (item in count) print count[item], item}' access.log | sort -nr | head -n 1 >> report.txt
echo "Отчет сохранен в report.txt"
