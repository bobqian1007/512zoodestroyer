#!/bin/bash

if [[ -z "$ZOOBINDIR" ]]
then
	echo "Error!! ZOOBINDIR is not set" 1>&2
	exit 1
fi

. $ZOOBINDIR/zkEnv.sh

# TODO Include your ZooKeeper connection string here. Make sure there are no spaces.
# 	Replace with your server names and client ports.
export ZKSERVER=lab1.cs.mcgill.ca:218XX,lab2.cs.mcgill.ca:218XX,lab3.cs.mcgill.ca:218XX

java -cp $CLASSPATH:../task:.: DistClient "$@"
