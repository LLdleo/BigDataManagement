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

public class CustomerTransactionDetailed {
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {

        @Override
        public void map(LongWritable key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            String filePath = ((FileSplit)reporter.getInputSplit()).getPath().toString();
            String line = value.toString();
            IntWritable custID = new IntWritable();

            if (filePath.contains("Customers")) {
                String[] values = line.split(",");

                custID.set(Integer.parseInt(values[0]));
                String custName = values[1];
                String salary = values[5];

                output.collect(custID, new Text("CI#" + custName + "," + salary));
            }

            else if (filePath.contains("Transactions")) {
                String[] values = line.split(",");

                custID.set(Integer.parseInt(values[1]));
                String transTotal = values[2];
                String transNumItems = values[3];

                output.collect(custID, new Text("TT#" + transTotal + "," + transNumItems));
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {

        @Override
        public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
            String customerInfo = "";
            List<Float> transTotal = new ArrayList<>();
            List<Integer> transNumItems = new ArrayList<>();

            while (values.hasNext()) {
                String value = values.next().toString();
                if (value.startsWith("CI#")) {
                    customerInfo = value.substring(3);
                } else if (value.startsWith("TT#")) {
                    String[] transInfo = value.substring(3).split(",");
                    transTotal.add(Float.valueOf(transInfo[0]));
                    transNumItems.add(Integer.valueOf(transInfo[1]));
                }
            }

            int transNum = transTotal.size();
            double totalSum = 0.00;
            int minItems = 0;

            if (transNum > 0) {
                minItems = 10;
                for (int i = 0; i < transNum; i++) {
                    totalSum += transTotal.get(i);
                    int transItems = transNumItems.get(i);
                    if (transItems < minItems) {
                        minItems = transItems;
                    }
                }
                totalSum = (float) (Math.round(totalSum * 100))/100;
            }

            output.collect(key, new Text(customerInfo + "," + transNum + "," + totalSum + "," + minItems));
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(CustomerTransactionDetailed.class);
        conf.setJobName("CustomerTransactionDetailed");
        conf.setOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(Map.class);
//        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
