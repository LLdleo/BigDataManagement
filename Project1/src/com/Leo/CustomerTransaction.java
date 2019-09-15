package com.Leo;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CustomerTransaction {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {

        @Override
        public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            String filePath = ((FileSplit)reporter.getInputSplit()).getPath().toString();
            String  line = value.toString();
            IntWritable custID = new IntWritable();

            if (filePath.contains("Customers")) {
                String[] values = line.split(",");

                custID.set(Integer.parseInt(values[0]));
                String custName = values[1];

                output.collect(custID, new Text("name#"+custName));
            }

            else if (filePath.contains("Transactions")) {
                String[] values = line.split(",");

                custID.set(Integer.parseInt(values[1]));
                String transTotal = values[2];

                output.collect(custID, new Text("TT#" + transTotal));
            }
        }

    }

    public static class MyCombiner extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            List<Float> transTotalRecord = new ArrayList<>();

            while (values.hasNext()) {
                String value = values.next().toString();
                if (value.startsWith("name#")) {
                    output.collect(key, new Text(value));
                    return;
                }
                else if (value.startsWith("TT#")) {
                    transTotalRecord.add(Float.parseFloat(value.substring(3)));
                }

                int transNum = transTotalRecord.size();
                double totalSum = 0.00;

                if (transNum > 0) {
                    for (int i = 0; i < transNum; i++) {
                        totalSum += transTotalRecord.get(i);
                    }
                    totalSum = (float) (Math.round(totalSum * 100))/100;
                    output.collect(key, new Text("T#" + transNum + "," + totalSum));
                }
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {

        @Override
        public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            String name = "";
            String  trans = "";

            while (values.hasNext()) {
                String value = values.next().toString();
                if (value.startsWith("name#")) {
                    name = value.substring(5);
                } else if (value.startsWith("T#")) {
                    trans = value.substring(2);
                }
            }

            output.collect(key, new Text(name + "," + trans));
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(CustomerTransaction.class);
        conf.setJobName("customerTransactions");
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
