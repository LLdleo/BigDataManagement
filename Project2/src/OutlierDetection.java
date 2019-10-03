import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class OutlierDetection {
    private static int r;
    private static int k;
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String[] valueSplit = value.toString().split(",");
            int xp = Integer.parseInt(valueSplit[0]);
            int yp = Integer.parseInt(valueSplit[1]);
            int xo = xp / (2*r);
            int yo = yp / (2*r);
            if (xp==10000) {
                xo --;
            }
            if (yp==10000) {
                yo --;
            }
            output.collect(new Text(xo + "," + yo), value);
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            List<String> inSectionList = new ArrayList<>();
            List<Integer> outlierNumList = new ArrayList<>();

            while (values.hasNext()) {
                String value = values.next().toString();
                inSectionList.add(value);
            }
            int sectionSize = inSectionList.size();
            for (int i=0; i<sectionSize;i++) {
                int numThreshold = 0;
                for (int j=0; j<sectionSize;j++) {
                    if (i!=j) {
                        if (distanceBeyond(inSectionList.get(i), inSectionList.get(j), r)) {
                            if (!outlierNumList.contains(i)) {
                                outlierNumList.add(i);
                            }
                        }
                        else {
                            numThreshold += 1;
                        }
                        if (numThreshold>=k) {
                            if (outlierNumList.contains(i)) {
                                outlierNumList.remove((Integer) i);
                            }
                            break;
                        }
                    }
                }
            }
            for (int outlierNum :outlierNumList) {
                output.collect(new Text(inSectionList.get(outlierNum)), new Text(""));
            }
        }

        boolean distanceBeyond(String point1, String point2, int distanceThreshold) {
            String[] p1Split = point1.split(",");
            String[] p2Split = point2.split(",");
            int x1p = Integer.parseInt(p1Split[0]);
            int y1p = Integer.parseInt(p1Split[1]);
            int x2p = Integer.parseInt(p2Split[0]);
            int y2p = Integer.parseInt(p2Split[1]);
            double distance = Math.sqrt(Math.pow(x1p - x2p, 2) + Math.pow(y1p - y2p, 2));
            return distance >= distanceThreshold;
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(OutlierDetection.class);
        conf.setMemoryForReduceTask(4096);
//        System.out.println(memoryLimit);
        conf.setJobName("OutlierDetection");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(Map.class);
//        conf.setCombinerClass(Reduce.class);
//        conf.setNumReduceTasks(1);
        conf.setReducerClass(Reduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        r = Integer.parseInt(args[2]);
        k = Integer.parseInt(args[3]);
        System.out.println(r + "," + k);

        JobClient.runJob(conf);
    }
}
