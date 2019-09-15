package com.Leo;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.*;

public class CountryCustomerTrans {
    private HashMap custIDTOCountryCode = new HashMap();

    public static class KeyValue extends HashMap{
        java.util.Map<IntWritable, IntWritable> custIDToCountryCode = new HashMap<>();

        java.util.Map<IntWritable, IntWritable> getCustIDToCountryCode() {
            return custIDToCountryCode;
        }

        void setCustIDToCountryCode(java.util.Map<IntWritable, IntWritable> custIDToCountryCode) {
            this.custIDToCountryCode = custIDToCountryCode;
        }
    }

    private static KeyValue KV = new KeyValue();


    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {
        java.util.Map<IntWritable, IntWritable> CITCC = new HashMap<>();

        @Override
        public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            String filePath = ((FileSplit)reporter.getInputSplit()).getPath().toString();
            String  line = value.toString();
            IntWritable custID = new IntWritable();

            if (filePath.contains("Customers")) {
                String[] values = line.split(",");

                custID.set(Integer.parseInt(values[0]));
                String countryCode = values[4];
                CITCC.put(custID, new IntWritable(Integer.parseInt(countryCode)));

                output.collect(custID, new Text("CC#" + countryCode));
            }

            else if (filePath.contains("Transactions")) {
                String[] values = line.split(",");

                custID.set(Integer.parseInt(values[1]));
                String transTotal = values[2];

                output.collect(custID, new Text("TT#" + transTotal));
            }
            KV.setCustIDToCountryCode(CITCC);
        }
    }

    public static class MyCombiner extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
//        private IdentityHashMap<IntWritable, IntWritable> custIDToCountryCode;
//
//        public MyCombiner(IdentityHashMap<IntWritable, IntWritable> custIDToCountryCode) {
//            this.custIDToCountryCode = custIDToCountryCode;
//        }
        java.util.Map<IntWritable, IntWritable> CITCC = KV.getCustIDToCountryCode();

        @Override
        public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            List<Float> transTotalRecord = new ArrayList<>();
            IntWritable countryCode = new IntWritable();

            while (values.hasNext()) {
                String value = values.next().toString();
                if (value.startsWith("CC#")) {
                    countryCode.set(Integer.parseInt(value.substring(3)));
                    output.collect(countryCode, new Text("CI#" + key));
                }
                else if (value.startsWith("TT#")) {
                    transTotalRecord.add(Float.parseFloat(value.substring(3)));
                }
            }

            int transNum = transTotalRecord.size();

            if (transNum > 0) {
                for (int i = 0; i < transNum; i++) {
                    Float transTotal = transTotalRecord.get(i);
                    output.collect(CITCC.get(key), new Text("TT#" + transTotal));
                }
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {

        @Override
        public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            List<Float> transTotalRecord = new ArrayList<>();
            int customerNumber = 0;

            while (values.hasNext()) {
                String value = values.next().toString();
                if (value.startsWith("CI#")) {
                    customerNumber += 1;
                }
                else if (value.startsWith("TT#")) {
                    transTotalRecord.add(Float.parseFloat(value.substring(3)));
                }
            }

            int transNum = transTotalRecord.size();
            double maxTotal = 10.00;
            double minTotal = 1000.00;

            if (transNum > 0) {
                for (int i = 0; i < transNum; i++) {
                    Float transTotal = transTotalRecord.get(i);
                    if (maxTotal < transTotal) {
                        maxTotal = transTotal;
                    }
                    if (minTotal > transTotal) {
                        minTotal = transTotal;
                    }
                }
                output.collect(key, new Text(customerNumber + "," + minTotal + "," + maxTotal));
            }
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

        JobClient.runJob(conf);
    }
}
