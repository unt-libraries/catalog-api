#!/bin/bash
pid_file=pids.txt
if [ ! -f $pid_file ]
    then
        echo "Servers are already stopped."
    else
        while read pid
        do
            pkill -TERM -P $pid
            kill -TERM $pid
        done < $pid_file
        rm $pid_file
        echo "Servers stopped successfully."
fi
