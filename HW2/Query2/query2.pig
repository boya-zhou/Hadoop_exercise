/*query2.pig*/

trans_raw = LOAD '/usr/share/hadoop/hw1/Transactions.txt' USING PigStorage(',') 
	AS (TransID,CusID,TransTotal,TransNumItems,TransDesc);
    
cus_raw = LOAD '/usr/share/hadoop/hw1/Customers.txt' USING PigStorage(',') 
	AS (CusID,Name,Age,CountryCode,Salary);
    
trans_1 = foreach trans_raw generate TransID, CusID, TransTotal, TransNumItems;
    
cus_1 = foreach cus_raw generate CusID, Name, Salary;

/*group transactions first*/

trans_group = group trans_1 by CusID;

trans_group = foreach trans_group 
	generate group as CusID, COUNT(trans_1) as NumOfTransactions,SUM(trans_1.TransTotal)
    as TotalSum, MIN(trans_1.TransNumItems) as MinItems;
    
cus_trans = join cus_1 by CusID, trans_group by CusID;

cus_trans_final = foreach cus_trans generate $0, Name, Salary, NumOfTransactions,TotalSum,MinItems;

STORE cus_trans_final INTO '/user/hadoop/output/hw2_output_2.txt' USING PigStorage(',');
