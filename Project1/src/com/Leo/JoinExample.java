package com.Leo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.Vector;

/**
 * MapReduce实现Join操作
 */
public class JoinExample {
    public static final String DELIMITER = "\u0009"; // 字段分隔符

    // map过程
    public static class MapClass extends MapReduceBase implements
            Mapper<LongWritable, Text, Text, Text> {

        public void configure(JobConf job) {
            super.configure(job);
        }

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output,
                        Reporter reporter) throws IOException, ClassCastException {
            // 获取输入文件的全路径和名称
            String filePath = ((FileSplit)reporter.getInputSplit()).getPath().toString();
            // 获取记录字符串
            String line = value.toString();
            // 抛弃空记录
            if (line == null || line.equals("")) return;

            // 处理来自表A的记录
            if (filePath.contains("m_ys_lab_jointest_a")) {
                String[] values = line.split(DELIMITER); // 按分隔符切割出字段
                if (values.length < 2) return;

                String id = values[0]; // id
                String name = values[1]; // name

                output.collect(new Text(id), new Text("a#"+name));
            }
            // 处理来自表B的记录
            else if (filePath.contains("m_ys_lab_jointest_b")) {
                String[] values = line.split(DELIMITER); // 按分隔符切割出字段
                if (values.length < 3) return;

                String id = values[0]; // id
                String statyear = values[1]; // statyear
                String num = values[2]; //num

                output.collect(new Text(id), new Text("b#"+statyear+DELIMITER+num));
            }
        }
    }

    // reduce过程
    public static class Reduce extends MapReduceBase
            implements Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterator<Text> values,
                           OutputCollector<Text, Text> output, Reporter reporter)
                throws IOException {

            Vector<String> vecA = new Vector<String>(); // 存放来自表A的值
            Vector<String> vecB = new Vector<String>(); // 存放来自表B的值

            while (values.hasNext()) {
                String value = values.next().toString();
                if (value.startsWith("a#")) {
                    vecA.add(value.substring(2));
                } else if (value.startsWith("b#")) {
                    vecB.add(value.substring(2));
                }
            }

            int sizeA = vecA.size();
            int sizeB = vecB.size();

            // 遍历两个向量
            int i, j;
            for (i = 0; i < sizeA; i ++) {
                for (j = 0; j < sizeB; j ++) {
                    output.collect(key, new Text(vecA.get(i) + DELIMITER +vecB.get(j)));
                }
            }
        }
    }

    protected void configJob(JobConf conf) {
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setOutputFormat(TextOutputFormat.class);
    }
}
