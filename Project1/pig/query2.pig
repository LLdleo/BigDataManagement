customers = load '/user/hadoop/input/Customers/Customers' using PigStorage(',') as (custID:int, name:chararray, age:int, gender: chararray, countrycode:int, salary:float);
--customers = load '/user/hadoop/input/tests/Customers_test' using PigStorage(',') as (custID:int, name:chararray, age:int, gender: chararray, countrycode:int, salary:float);

transactions = load '/user/hadoop/input/Transactions/Transactions' using PigStorage(',') as (transID:int, custID:int, tranTotal:float, transNumItems:int, transDesc:chararray);
--transactions = load '/user/hadoop/input/tests/Transactions_test' using PigStorage(',') as (transID:int, custID:int, transTotal:float, transNumItems:int, transDesc:chararray);

idToName = FOREACH customers GENERATE custID,name;

transGroup = GROUP transactions By (custID);
IDTransaction = FOREACH transGroup GENERATE group as custID, COUNT(transactions.transID) as transNum, SUM(transactions.transTotal) as totalSum, MIN(transactions.transNumItems) as minItems;

result = JOIN customers BY custID LEFT OUTER, IDTransaction BY custID USING 'repliacated';
-- result2 = FOREACH result GENERATE customers.custID, customers.name, customers.salary, IDTransaction.transNum, IDTransaction.totalSum, IDTransaction.minItems;
result2 = FOREACH result GENERATE $0, $1, $5, $6, $7, $8;

STORE result2 INTO '/user/hadoop/output/query2' USING PigStorage(',');
