#!/bin/bash
# Ask the user for login details
read -p 'input file: ' input
read -p 'output file: ' output
read -p 'server name of the master node: (eg c220g2-011314vm-1.wisc.cloudlab.us)' servername
./spark-2.2.0-bin-hadoop2.7/bin/spark-submit  --master spark://${servername}:7077 sort.py $input $output