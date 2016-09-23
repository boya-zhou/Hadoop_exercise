#!/usr/bin/env python
 
import sys

CusID=0
CusName = "" #default sorted as first
TransNum = 1 #default sorted as first
CountryCode=0

 
# input comes from STDIN (standard input)
for line in sys.stdin:
 
    
    # remove leading and trailing whitespace
    line = line.strip()

    splits = line.split(",")

    #       Customers data, and put a label "0"
    if len(splits[1]) > 9: 
        CusID = int(splits[0])
        CusName = splits[1]
        CountryCode=int(splits[3])
        if CountryCode == 5:
            print '%s\t%s\t%s' %(CusID,"0",CusName)
    #       Transactionss data, and put a label "1"
    else: 
        CusID = int(splits[1])
        TransNum = 1
        print '%s\t%s\t%s' %(CusID,'1',1)
