package com.Leo;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class DividedByAgeGender {

//    public static KeyValues KVs = new KeyValues();

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
//            java.util.Map<Text, Text> CITAG = KVs.custIDToAgeGender;
            String filePath = ((FileSplit)reporter.getInputSplit()).getPath().toString();
            String line = value.toString();
            String  custID = "";

            if (filePath.contains("Customers")) {
                String[] values = line.split(",");
                custID = values[0];
                String age = values[2];
                String gender = values[3];

                output.collect(new Text(custID), new Text("CI#" + age + "," + gender));
            }

            else if (filePath.contains("Transactions")) {
                String[] values = line.split(",");
                custID = values[1];
                String transID = values[0];
                String transTotal = values[2];

                output.collect(new Text(custID), new Text("TI#" + transID + "," + transTotal));
            }
        }

    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            List<String> transTotalRecord = new ArrayList<>();
            String age = "";
            String gender = "";

            while (values.hasNext()) {
                String value = values.next().toString();
                if (value.startsWith("CI#")) {
                    String[] line = value.substring(3).split(",");
                    age = line[0];
                    gender = line[1];
                }
                else if (value.startsWith("TI#")) {
                    transTotalRecord.add(value.substring(3));
                }
            }
            int transNum = transTotalRecord.size();

            if (transNum > 0) {
                for (String s : transTotalRecord) {
                    output.collect(new Text(age + "," + gender), new Text(s));
                }
            }
        }
    }

    public static class Map2 extends MapReduceBase implements Mapper<Text, Text, Text, Text> {

        @Override
        public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String[] keyLine = key.toString().split(",");
            int age = Integer.parseInt(keyLine[0]);
            String gender = keyLine[1];

            if (age >= 10 & age <20) {
                if (gender.equals("Male")) {
                    output.collect(new Text("Tens Male"), new Text(value));
                }
                else if (gender.equals("Female")) {
                    output.collect(new Text("Tens Female"), new Text(value));
                }
            }
            else if (age >= 20 & age <30) {
                if (gender.equals("Male")) {
                    output.collect(new Text("Twenties Male"), new Text(value));
                }
                else if (gender.equals("Female")) {
                    output.collect(new Text("Twenties Female"), new Text(value));
                }
            }
            else if (age >= 30 & age <40) {
                if (gender.equals("Male")) {
                    output.collect(new Text("Thirties Male"), new Text(value));
                }
                else if (gender.equals("Female")) {
                    output.collect(new Text("Thirties Female"), new Text(value));
                }
            }
            else if (age >= 40 & age <50) {
                if (gender.equals("Male")) {
                    output.collect(new Text("Forties Male"), new Text(value));
                }
                else if (gender.equals("Female")) {
                    output.collect(new Text("Forties Female"), new Text(value));
                }
            }
            else if (age >= 50 & age <60) {
                if (gender.equals("Male")) {
                    output.collect(new Text("Fifties Male"), new Text(value));
                }
                else if (gender.equals("Female")) {
                    output.collect(new Text("Fifties Female"), new Text(value));
                }
            }
            else if (age >= 60 & age <=70) {
                if (gender.equals("Male")) {
                    output.collect(new Text("Sixties Male"), new Text(value));
                }
                else if (gender.equals("Female")) {
                    output.collect(new Text("Sixties Female"), new Text(value));
                    }
            }

        }
    }

    public static class Reduce2 extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            List<Float> transTotalRecord = new ArrayList<>();
            float avgTransTotal = 0;

            while (values.hasNext()) {
                String value = values.next().toString();
                transTotalRecord.add(Float.valueOf(value.split(",")[1]));
            }

            int transNum = transTotalRecord.size();
            float minTransTotal = 1000;
            float maxTransTotal = 10;
            float sumTransTotal = 0;


            if (transNum > 0) {
                for (float transTotal : transTotalRecord) {
                    sumTransTotal += transTotal;
                    if (minTransTotal > transTotal) {
                        minTransTotal = transTotal;
                    }
                    if (maxTransTotal < transTotal) {
                        maxTransTotal = transTotal;
                    }
                }
                avgTransTotal = (float) Math.round(sumTransTotal / transNum * 100) / 100;

            }
            output.collect(key, new Text(minTransTotal + "," + maxTransTotal + "," + avgTransTotal));
        }
    }

    public static void main(String[] args) throws Exception {
        Path transitPath = new Path(args[1].replace("DividedByAgeGender", "transitPath"));

        JobConf conf = new JobConf(DividedByAgeGender.class);
        conf.setJobName("DividedByAgeGender");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, transitPath);
//        KVs.setcustIDToAgeGender(args[0]);

        JobClient.runJob(conf);

        JobConf conf2 = new JobConf(DividedByAgeGender.class);
        conf2.setJobName("DividedByAgeGender");
        conf2.setOutputKeyClass(Text.class);
        conf2.setOutputValueClass(Text.class);
        conf2.setMapperClass(Map2.class);
        conf.setCombinerClass(Reduce2.class);
        conf2.setReducerClass(Reduce2.class);
        conf2.setInputFormat(TextInputFormat.class);
        conf2.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf2, transitPath);
        FileOutputFormat.setOutputPath(conf2, new Path(args[1]));

        JobClient.runJob(conf2);

    }
}

class KeyValues1 extends HashMap {
    public java.util.Map<Text, Text> custIDToAgeGender = new HashMap<>();

    void setcustIDToAgeGender(String arg) throws IOException {

        String line = null;
        int age = 0;
        String gender = "";
        java.util.Map<Text, Text> custIDToAgeGender = new HashMap<>();
        FileInputStream fileInputStream = new FileInputStream(arg + "/Customers");
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));
        while((line = bufferedReader.readLine()) != null) {
            String[] values = line.split(",");
            age = Integer.parseInt(values[2]);
            gender = values[3];
            custIDToAgeGender.put(new Text(values[0]), new Text(values[2] + "," + values[3]));
        }
        this.custIDToAgeGender = custIDToAgeGender;
    }
}