#!/bin/sh

if [ $# -ne 2 ];
then
    echo "Invalid number of arguments"
    exit 0
fi

while true;
do
    SIZE=$(pmap $1|grep total|grep -o "[0-9]*")
    # SIZE=$(ps -o rss= -p $1)
    SIZE=$( echo $SIZE | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')
    echo "Process id =$1 Size =-$SIZE KB"
    if [ $SIZE -gt $2 ]; then
        printf "SIZE has exceeded.\nKilling the process......"
        kill -15 "$1"
        echo "Killed the process"
        exit 0
    else
        echo "SIZE has not yet exceeding"
    fi
done

