package com.Leo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

public class Query2 {
    static class MyMapper extends Mapper<Object, Text, Text, Text> {

        private HashMap idandName = new HashMap();


        protected void setup(Context context) throws IOException, InterruptedException {
            BufferedReader br = null;
            Path[] distributePaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            String customer = null;

            for (Path p : distributePaths) {
                if (p.toString().contains("Customer")) {
                    br = new BufferedReader(new FileReader(p.toString()));
                    while (null != (customer = br.readLine())) {
                        String[] customer_info = customer.split(",");
                        idandName.put(customer_info[0], customer_info[1]);
                    }
                }
            }
        }

        private Text outPutKey = new Text();
        private Text outPutValue = new Text();
        private Object name = null;

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] l = value.toString().split(",");
            name = idandName.get(l[1]);
            outPutKey.set(l[1]); //id
            outPutValue.set(name + "," + l[2]); //the output is name transtotal
            context.write(outPutKey, outPutValue); // id name transtotal
        }
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            float totalSum = 0.0f;
            String name = "";
            int sum = 0;

            for (Text v : values) {
                String[] l = v.toString().split(",");
                name = l[0];
                totalSum += Float.parseFloat(l[1]);
                sum += 1;
            }

            //result.set(/*numTransaction + "\t" + */String.valueOf(sum));
            context.write(key, new Text(name + "," + sum + "," + totalSum));

        }
    }

    public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
            Configuration conf = new Configuration();
            Job job = new Job(conf, "Distributed Cache");
            job.setJarByClass(Query2.class);
            job.setMapperClass(MyMapper.class);
            job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            DistributedCache.addCacheFile(new URI("E:/javaProject/DS503/Project1/Customers_test"), job.getConfiguration());
            //job.setNumReduceTasks(0);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            job.waitForCompletion(true);
    }

}


