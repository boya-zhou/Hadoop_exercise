A = load '/usr/share/hadoop/hw1/Transactions.txt' using PigStorage(',') as(TransID:int,CustID:int, TransTotal:float, TransNumItems:int, TransDesc: chararray);
B = load '/usr/share/hadoop/hw1/Customers.txt' using PigStorage(',') as(ID:int, Name:chararray, Age:int, CountryCode:int, Salary: float);
C = join A by $1, B by $0;
D = group C by B::CountryCode;
E = foreach D {F = DISTINCT C.ID; generate group, COUNT(F), MIN(C.TransTotal), MAX(C.TransTotal);};
store E into 'pig3' using PigStorage(',');
