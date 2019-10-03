
rm -r query3_classes
mkdir query3_classes
javac -classpath .:/usr/share/hadoop/hadoop-core-1.2.1.jar:/home/hadoop/Downloads/fastjson-1.2.58.jar -d query3_classes ./*.java
jar -cvf ./query3.jar -C query3_classes/ .
hadoop dfs -rmr /user/hadoop/output
hadoop jar query3.jar Query3 /user/hadoop/input/airfield.json /user/hadoop/output/

javac -encoding utf-8 -Xlint:unchecked -classpath C:/Java/jdk/lib/hadoop-core-1.2.1.jar;C:/Java/jdk/lib/hadoop-mapreduce-client-core-2.7.3.jar;C:/Java/jdk/lib/commons-logging-1.2.jar -d class/FlagAggregate_classes src/FlagAggregate.java src/JsonFileInputFormat.java

jar -cvf jar/FlagAggregate.jar -C class/FlagAggregate_classes/ .

hadoop jar jar/FlagAggregate.jar FlagAggregate fileInput/airField/ fileOutput/FlagAggregate/

