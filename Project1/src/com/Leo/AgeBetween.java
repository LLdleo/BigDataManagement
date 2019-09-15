package com.Leo;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class AgeBetween {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, NullWritable> {
        private Text line = new Text();

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, NullWritable> output, Reporter reporter) throws IOException {
            line.set(value.toString());
            String record = line.toString();
            String[] lineSplit = record.split(",");   // read one line and split it
            IntWritable age = new IntWritable();
            age.set(Integer.parseInt(lineSplit[2]));
            if (age.get() >= 20 & age.get() <= 50){
                output.collect(line, NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(AgeBetween.class);
        conf.setJobName("ageBetween");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(NullWritable.class);
        conf.setMapperClass(Map.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
