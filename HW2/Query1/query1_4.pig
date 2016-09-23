/* query1.pig*/

raw = LOAD '/usr/share/hadoop/hw1/Transactions.txt' USING PigStorage(',') AS (TransID, CusID, TransTotal, NumItems, TransDesc);

/*clean1 = FOREACH raw GENERATE TransID,CusID,Transtotal;*/

user_group = GROUP raw BY CusID;

user_group_counts = FOREACH user_group
	Generate group, COUNT(raw), SUM(raw.TransTotal);
    

STORE user_group_counts INTO '/user/hadoop/output/hw2_output_1.txt' USING PigStorage(',');
