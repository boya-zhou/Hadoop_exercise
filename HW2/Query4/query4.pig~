/*query4.pig*/

trans_raw = LOAD '/usr/share/hadoop/hw1/Transactions.txt' USING PigStorage(',') 
	AS (TransID,CusID,TransTotal,TransNumItems,TransDesc);
    
cus_raw = LOAD '/usr/share/hadoop/hw1/Customers.txt' USING PigStorage(',') 
	AS (CusID,Name,Age,CountryCode,Salary);
    
trans_1 = foreach trans_raw generate TransID, CusID;
    
cus_1 = foreach cus_raw generate CusID, Name;

/*group transactions*/

trans_group = group trans_1 by CusID;

trans_2 = foreach trans_group 
	generate group as CusID, COUNT(trans_1) as NumOfTrans;
    
trans_3 = GROUP trans_2 ALL;
trans_4 = FOREACH trans_3 GENERATE MIN(trans_2.NumOfTrans) AS val;
trans_5 = FILTER trans_2 BY NumOfTrans == trans_4.val;


cus_trans = join cus_1 by CusID, trans_5 by CusID using 'replicated';

cus_trans_final = foreach cus_trans generate Name, NumOfTrans;


STORE cus_trans_final INTO '/user/hadoop/output/hw2_output_4.txt' USING PigStorage(',');
