#!/bin/bash

for i in {1..10}
do
  echo "Running" $i
 	. ./$SCRIPT.sh >> out/$SCRIPT.out
        echo "\n" >> out/$SCRIPT.out
	sleep $SLEEP
done
