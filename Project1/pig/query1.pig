customers = load '/user/hadoop/input/Customers/Customers' using PigStorage(',') as (custID:int, name:chararray, age:int, gender: chararray, countrycode:int, salary:float);
-- customers = load '/user/hadoop/input/tests/Customers_test' using PigStorage(',') as (custID:int, name:chararray, age:int, gender: chararray, countrycode:int, salary:float);

transactions = load '/user/hadoop/input/Transactions/Transactions' using PigStorage(',') as (transID:int, custID:int, transTotal:float, transNumItems:int, transDesc:chararray);
-- transactions = load '/user/hadoop/input/tests/Transactions_test' using PigStorage(',') as (transID:int, custID:int, transTotal:float, transNumItems:int, transDesc:chararray);

idToName = FOREACH customers GENERATE custID,name;

custIDToTransID = FOREACH transactions GENERATE custID,transID;

IDTransGroup = GROUP custIDToTransID BY (custID);

IDTransNum = FOREACH IDTransGroup GENERATE group as custID, COUNT(custIDToTransID.transID) as transNum;

custNameToTransNum = JOIN IDTransNum BY custID LEFT OUTER, idToName BY custID;

nameTransNum = FOREACH custNameToTransNum GENERATE name, transNum;

ntng = GROUP nameTransNum BY (transNum);
ntn1 = ORDER ntng BY group ASC;
ntn2 = LIMIT ntn1 1;
ntnr = FOREACH ntn2 GENERATE $0, $1;
--this line seems no effect, the results are just like ntn2

--STORE ntn2 INTO '/user/hadoop/output/query1' USING PigStorage(',');
STORE ntnr INTO '/user/hadoop/output/query1' USING PigStorage(',');
