#!/usr/bin/env python
from operator import itemgetter
import sys
# lastcusId is the cusId of last tuple

lastCusId = None
Transnum = 0
cusId = None
cusName = None
flag =False

for line in sys.stdin:
#     if line.strip()=="":
#         continue
    line = line.strip()
    splits = line.split("\t")
    cusId = splits[0]

    if cusId==lastCusId:
#check the label from mapper, to decide if is from customers or transactions
#but right now, the stdin from transactions contain tuple which countrycode not equal to 0
        
        if splits[1]=="0":
            flag = True
            cusName=splits[2]
        if splits[1]=="1":
            Transnum= Transnum + int(splits[2])
    else:
# since I fliter countrycode==5 in mapper, so I check, only the group with a label "0"
# can be aggregate and stdout, if do not check, the cusId in transactions with countrycode do not equal to 5
# will also be stdout
        if flag:          
            if lastCusId:
                print '%s\t%s\t%s' %(lastCusId,cusName,Transnum)
                Transnum = 0
                flag=False
            
        lastCusId = cusId
        if splits[1]=="0":
            cusName=splits[2]
        if splits[1]=="1":
            Transnum= int(splits[2]) 

        
            
if cusId==lastCusId:
    print '%s\t%s\t%s' %(lastCusId,cusName,Transnum)
