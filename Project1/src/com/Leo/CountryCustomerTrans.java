package com.Leo;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class CountryCustomerTrans {

    public static KeyValue KV = new KeyValue();

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
        java.util.Map<IntWritable, IntWritable> CITCC = KV.custIDToCountryCode;

//        public java.util.Map<IntWritable, IntWritable> custIDToCountryCode = new HashMap<>();
//
//        protected void setup(Context context) throws IOException {
//            BufferedReader br = null;
//            String line = null;
//
//            Path[] distributePaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
//            for(Path p : distributePaths){
//                if(p.toString().contains("Customers")){
//                    br = new BufferedReader(new FileReader(p.toString()));
//                    while((line=br.readLine()) != null){
//                        String[] values = line.split(",");
//                        custIDToCountryCode.put(new IntWritable(Integer.parseInt(values[0])), new IntWritable(Integer.parseInt(values[4]));
//
//                    }
//                }
//            }
//        }

        @Override
        public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            String filePath = ((FileSplit)reporter.getInputSplit()).getPath().toString();
            String  line = value.toString();
            IntWritable custID = new IntWritable();

            if (filePath.contains("Customers")) {
                String[] values = line.split(",");
                custID.set(Integer.parseInt(values[0]));
                String countryCode = values[4];
                output.collect(new IntWritable(Integer.parseInt(countryCode)), new Text("CI#" + custID));
            }

            if (filePath.contains("Transactions")) {
                String[] values = line.split(",");

                custID.set(Integer.parseInt(values[1]));
                String transTotal = values[2];

                output.collect(CITCC.get(custID), new Text("TT#" + transTotal));
            }
        }
    }

    public static class MyCombiner extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
//        private IdentityHashMap<IntWritable, IntWritable> custIDToCountryCode;
//
//        public MyCombiner(IdentityHashMap<IntWritable, IntWritable> custIDToCountryCode) {
//            this.custIDToCountryCode = custIDToCountryCode;
//        }
//        java.util.Map<IntWritable, IntWritable> CITCC = KV.getCustIDToCountryCode();

        @Override
        public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            List<Float> transTotalRecord = new ArrayList<>();
            IntWritable countryCode = new IntWritable();
            int countryCustNum = 0;

            while (values.hasNext()) {
                String value = values.next().toString();
                if (value.startsWith("CI#")) {
                    countryCustNum += 1;
                }
                else if (value.startsWith("TT#")) {
                    transTotalRecord.add(Float.parseFloat(value.substring(3)));
                }
            }
            if (countryCustNum > 0) {
                output.collect(key, new Text("CCN," + countryCustNum));
            }

            int transNum = transTotalRecord.size();
            float maxTotal = 10;
            float minTotal = 1000;

            if (transNum > 0) {
                for (Float transTotal : transTotalRecord) {
                    if (maxTotal < transTotal) {
                        maxTotal = transTotal;
                    }
                    if (minTotal > transTotal) {
                        minTotal = transTotal;
                    }
                }
                output.collect(key, new Text("MM," + minTotal + "," + maxTotal));
            }

        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {

        @Override
        public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            String customerNumber = "";
            String minTotal = "";
            String maxTotal = "";

            while (values.hasNext()) {
                String value = values.next().toString();
                if (value.startsWith("CCN")) {
                    customerNumber = value.split(",")[1];
                }
                else if (value.startsWith("MM")) {
                    minTotal = value.split(",")[1];
                    maxTotal = value.split(",")[2];
                }
            }

            output.collect(key, new Text(customerNumber + "," + minTotal + "," + maxTotal));
        }

    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(CountryCustomerTrans.class);
        conf.setJobName("CountryCustomerTrans");
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(Map.class);
        conf.setCombinerClass(MyCombiner.class);
        conf.setReducerClass(Reduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        KV.setCustIDToCountryCode(args[0]);

        JobClient.runJob(conf);
    }
}

class KeyValue extends HashMap{
    public java.util.Map<IntWritable, IntWritable> custIDToCountryCode = new HashMap<>();

    void setCustIDToCountryCode(String arg) throws IOException {

        String line = null;
        java.util.Map<IntWritable, IntWritable> custIDToCountryCode = new HashMap<>();
        FileInputStream fileInputStream = new FileInputStream(arg + "/Customers");
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));
        while((line = bufferedReader.readLine()) != null) {
            String[] values = line.split(",");
            custIDToCountryCode.put(new IntWritable(Integer.parseInt(values[0])), new IntWritable(Integer.parseInt(values[4])));
        }
        this.custIDToCountryCode = custIDToCountryCode;
    }
}
