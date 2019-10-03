import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class OutlierDetectionDoubleCheck {
    private static int r;
    private static int k;
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String valueS = value.toString();
            String[] valueSplit = valueS.split(",");
            int xp = Integer.parseInt(valueSplit[0]);
            int yp = Integer.parseInt(valueSplit[1]);
            int xo = xp / (2*r);
            int yo = yp / (2*r);
            if (xp==10000) {
                xo = (int) (Math.ceil(xo) -1);
            }
            if (yp==10000) {
                yo = (int) (Math.ceil(yo) -1);
            }

            int xMinus = (xp-r) / (2*r);
            int xPlus = (xp+r) / (2*r);
            int yMinus = (yp-r) / (2*r);
            int yPlus = (yp+r) / (2*r);
            if ((xMinus != xo) & (xp > r) & (yMinus != yo) & (yp > r)) {
                output.collect(new Text(xMinus + "," + yMinus), new Text("O#" + valueS));
            }
            if ((xMinus != xo) & (xp > r) & (yPlus != yo) & (yp < 10000-r)) {
                output.collect(new Text(xMinus + "," + yMinus), new Text("O#" + valueS));
            }
            if ((xPlus != xo) & (xp < 10000-r) & (yMinus != yo) & (yp > r)) {
                output.collect(new Text(xMinus + "," + yMinus), new Text("O#" + valueS));
            }
            if ((xPlus != xo) & (xp < 10000-r) & (yMinus != yo) & (yp < 10000-r)) {
                output.collect(new Text(xMinus + "," + yMinus), new Text("O#" + valueS));
            }
            output.collect(new Text(xo + "," + yo), new Text("C#" + valueS));
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            List<String> inSectionCenterList = new ArrayList<>();
            List<String> inSectionAllList = new ArrayList<>();
            List<Integer> outlierNumList = new ArrayList<>();

            while (values.hasNext()) {
                String value = values.next().toString();
                if (value.startsWith("C#")) {
                    inSectionCenterList.add(value.substring(2));
                }
                if (value.startsWith("O#")) {
                    inSectionAllList.add(value.substring(2));
                }
            }
            int sectionSize = inSectionCenterList.size();
            int allPointSize = inSectionAllList.size();
            for (int i=0; i<sectionSize;i++) {
                int numThreshold = 0;
                for (int j=0; j<allPointSize;j++) {
                    if (i!=j) {
                        if (distanceBeyond(inSectionCenterList.get(i), inSectionAllList.get(j), r)) {
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
                output.collect(new Text(inSectionCenterList.get(outlierNum)), new Text(""));
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
        JobConf conf = new JobConf(OutlierDetectionDoubleCheck.class);
//        conf.setMemoryForReduceTask(4096);
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
