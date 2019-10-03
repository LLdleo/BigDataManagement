import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OutlierDetectionLoop {
    private static int r;
    private static int k;

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            int count = Integer.parseInt(context.getConfiguration().get("centersPath"));
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
            context.write(new Text(xo + "," + yo), value);
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> inSectionList = new ArrayList<>();
            List<Integer> outlierNumList = new ArrayList<>();

            for (Text value_: values) {
                String value = value_.toString();
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
                context.write(new Text(inSectionList.get(outlierNum)), new Text(""));
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

    public static void run(String inPath, String outPath, int count) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("number", ""+count);
        // set separator
        conf.set("mapred.textoutputformat.ignoreseparator", "true");
        conf.set("mapred.textoutputformat.separator", "#");

        Job job = new Job(conf, "mykmeans");
        job.setJarByClass(KMeansMapReduce.class);

        job.setMapperClass(Map.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path(inPath));
        org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, new Path(outPath));

        System.out.println(job.waitForCompletion(true));
    }

    public static void main(String[] args) throws Exception {
        String inPath = args[0];
        String outPath = args[1];
        String tempOutPath = "fileOutput/Centers";
        r = Integer.parseInt(args[2]);
        k = Integer.parseInt(args[3]);

        int[] n = {1,2};

        for (int count: n){
            run(inPath,tempOutPath + count, count);
            System.out.println(" This is the " + count + " count");
        }

    }
}
