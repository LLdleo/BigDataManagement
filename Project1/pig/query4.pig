customers = load '/user/hadoop/input/Customers/Customers' using PigStorage(',') as (custID:int, name:chararray, age:int, gender: chararray, countrycode:int, salary:float);
--customers = load '/user/hadoop/input/tests/Customers_test' using PigStorage(',') as (custID:int, name:chararray, age:int, gender: chararray, countrycode:int, salary:float);

transactions = load '/user/hadoop/input/Transactions/Transactions' using PigStorage(',') as (transID:int, custID:int, tranTotal:float, transNumItems:int, transDesc:chararray);
--transactions = load '/user/hadoop/input/tests/Transactions_test' using PigStorage(',') as (transID:int, custID:int, transTotal:float, transNumItems:int, transDesc:chararray);

tc = JOIN transactions BY custID LEFT OUTER, customers BY custID;
tcg = FOREACH tc GENERATE $7 as age, $8 as gender, $2 as transTotal;

SPLIT tcg INTO
m10 IF ((age>=10) and (age<20) and (gender == 'Male')), f10 IF ((age>=10) and (age<20) and (gender == 'Female')),
m20 IF ((age>=20) and (age<30) and (gender == 'Male')), f20 IF ((age>=20) and (age<30) and (gender == 'Female')),
m30 IF ((age>=30) and (age<40) and (gender == 'Male')), f30 IF ((age>=30) and (age<40) and (gender == 'Female')),
m40 IF ((age>=40) and (age<50) and (gender == 'Male')), f40 IF ((age>=40) and (age<50) and (gender == 'Female')),
m50 IF ((age>=50) and (age<60) and (gender == 'Male')), f50 IF ((age>=50) and (age<60) and (gender == 'Female')),
m60 IF ((age>=60) and (age<=70) and (gender == 'Male')), f60 IF ((age>=60) and (age<=70) and (gender == 'Female'));

m10g = GROUP m10 BY (1);
m20g = GROUP m20 BY (1);
m30g = GROUP m30 BY (1);
m40g = GROUP m40 BY (1);
m50g = GROUP m50 BY (1);
m60g = GROUP m60 BY (1);
f10g = GROUP f10 BY (1);
f20g = GROUP f20 BY (1);
f30g = GROUP f30 BY (1);
f40g = GROUP f40 BY (1);
f50g = GROUP f50 BY (1);
f60g = GROUP f60 BY (1);

rm10 = FOREACH m10g GENERATE 'Tens Male' as g, MIN(m10.transTotal) as min, MAX(m10.transTotal) as max, AVG(m10.transTotal) as avg;
rm20 = FOREACH m20g GENERATE 'Twenties Male' as g, MIN(m20.transTotal) as min, MAX(m20.transTotal) as max, AVG(m20.transTotal) as avg;
rm30 = FOREACH m30g GENERATE 'Thirties Male' as g, MIN(m30.transTotal) as min, MAX(m30.transTotal) as max, AVG(m30.transTotal) as avg;
rm40 = FOREACH m40g GENERATE 'Forties Male' as g, MIN(m40.transTotal) as min, MAX(m40.transTotal) as max, AVG(m40.transTotal) as avg;
rm50 = FOREACH m50g GENERATE 'Fifties Male' as g, MIN(m50.transTotal) as min, MAX(m50.transTotal) as max, AVG(m50.transTotal) as avg;
rm60 = FOREACH m60g GENERATE 'Sixties Male' as g, MIN(m60.transTotal) as min, MAX(m60.transTotal) as max, AVG(m60.transTotal) as avg;
rf10 = FOREACH f10g GENERATE 'Tens Female' as g, MIN(f10.transTotal) as min, MAX(f10.transTotal) as max, AVG(f10.transTotal) as avg;
rf20 = FOREACH f20g GENERATE 'Twenties Female' as g, MIN(f20.transTotal) as min, MAX(f20.transTotal) as max, AVG(f20.transTotal) as avg;
rf30 = FOREACH f30g GENERATE 'Thirties Female' as g, MIN(f30.transTotal) as min, MAX(f30.transTotal) as max, AVG(f30.transTotal) as avg;
rf40 = FOREACH f40g GENERATE 'Forties Female' as g, MIN(f40.transTotal) as min, MAX(f40.transTotal) as max, AVG(f40.transTotal) as avg;
rf50 = FOREACH f50g GENERATE 'Fifties Female' as g, MIN(f50.transTotal) as min, MAX(f50.transTotal) as max, AVG(f50.transTotal) as avg;
rf60 = FOREACH f60g GENERATE 'Sixties Female' as g, MIN(f60.transTotal) as min, MAX(f60.transTotal) as max, AVG(f60.transTotal) as avg;

rfm = UNION rm10, rm20, rm30, rm40, rm50, rm60, rf10, rf20, rf30, rf40, rf50, rf60;

STORE rfm INTO '/user/hadoop/output/query4' USING PigStorage(',');
