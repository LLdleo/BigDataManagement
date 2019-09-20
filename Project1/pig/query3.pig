customers = load '/user/hadoop/input/Customers/Customers' using PigStorage(',') as (custID:int, name:chararray, age:int, gender: chararray, countrycode:int, salary:float);
--customers = load '/user/hadoop/input/tests/Customers_test' using PigStorage(',') as (custID:int, name:chararray, age:int, gender: chararray, countrycode:int, salary:float);

ccf = FOREACH customers GENERATE countrycode, custID;
countryCodeGroup = GROUP ccf BY (countrycode);
countryCodeNum = FOREACH countryCodeGroup GENERATE group as countrycode, COUNT($1) as custNum;
 filterCountryCode = FILTER countryCodeNum BY (custNum > 5000) OR (custNum < 2000);
--filterCountryCode = FILTER countryCodeNum BY (custNum > 5) OR (custNum < 2);

result = FOREACH filterCountryCode GENERATE $0, $1;

STORE result INTO '/user/hadoop/output/query3' USING PigStorage(',');
