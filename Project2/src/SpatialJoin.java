import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SpatialJoin {
    static String window = "";

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            // output key:1, output value: points and rectangles
            String filePath = ((FileSplit)reporter.getInputSplit()).getPath().toString();
            String line = value.toString();

            if (filePath.contains("Points")) {
                if (window.equals("")) {
                    output.collect(new Text("1"), new Text("P#" + line));
//                    System.out.println("no limit");
                }
                else{
                    String[] pointSplit = line.split(",");
                    String[] wdSplit = window.split("#");
                    int xPosition = Integer.parseInt(pointSplit[0]);
                    int yPosition = Integer.parseInt(pointSplit[1]);
                    int wdBottomLeftX = Integer.parseInt(wdSplit[0]);
                    int wdBottomLeftY = Integer.parseInt(wdSplit[1]);
                    int wdHeight = Integer.parseInt(wdSplit[2]);
                    int wdWidth = Integer.parseInt(wdSplit[3]);
                    if ((xPosition - wdBottomLeftX <= wdWidth) & (xPosition - wdBottomLeftX >= 0) & (yPosition - wdBottomLeftY <= wdHeight) & (yPosition - wdBottomLeftY >= 0)) {
                        output.collect(new Text("1"), new Text("P#" + line));
                    }
                }

            }
            else if (filePath.contains("Rectangles")) {
                if (window.equals("")) {
                    output.collect(new Text("1"), new Text("R#" + line));
//                    System.out.println("no limit");
                }
                else{
//                    System.out.println("window="+window);
                    String[] rectangleSplit = line.split(",");
                    String[] wdSplit = window.split("#");
                    int bottomLeftX = Integer.parseInt(rectangleSplit[1]);
                    int bottomLeftY = Integer.parseInt(rectangleSplit[2]);
                    int height = Integer.parseInt(rectangleSplit[3]);
                    int width = Integer.parseInt(rectangleSplit[4]);
                    int wdBottomLeftX = Integer.parseInt(wdSplit[0]);
                    int wdBottomLeftY = Integer.parseInt(wdSplit[1]);
                    int wdHeight = Integer.parseInt(wdSplit[2]);
                    int wdWidth = Integer.parseInt(wdSplit[3]);
                    if ((bottomLeftX - wdBottomLeftX >= 0) & (wdBottomLeftX + wdWidth - bottomLeftX - width >= 0) & (bottomLeftY - wdBottomLeftY >= 0) & (wdBottomLeftY + wdHeight - bottomLeftY - height >= 0)) {
                        output.collect(new Text("1"), new Text("R#" + line));
                        System.out.println("limited");
                    }
                }

            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            List<String> points = new ArrayList<>();
            List<String> rectangles = new ArrayList<>();

            while (values.hasNext()) {
                String value = values.next().toString();
                if (value.startsWith("P#")) {
//                    System.out.println("It's a point");
                    points.add(value.substring(2));
                } else if (value.startsWith("R#")) {
//                    System.out.println("It's a rectangle");
                    rectangles.add(value.substring(2));
                }
            }

            for (String rectangle : rectangles) {
                if (!rectangle.equals("")){
//                    System.out.print("first step");
                    String[] rectangleSplit = rectangle.split(",");
                    String rNum = rectangleSplit[0];
                    int bottomLeftX = Integer.parseInt(rectangleSplit[1]);
                    int bottomLeftY = Integer.parseInt(rectangleSplit[2]);
                    int height = Integer.parseInt(rectangleSplit[3]);
                    int width = Integer.parseInt(rectangleSplit[4]);
                    for (String point : points) {
                        if (!point.equals("")) {
//                            System.out.print("second step");
                            String[] pointSplit = point.split(",");
                            int xPosition = Integer.parseInt(pointSplit[0]);
                            int yPosition = Integer.parseInt(pointSplit[1]);
                            if ((xPosition - bottomLeftX <= width) & (xPosition - bottomLeftX >= 0) & (yPosition - bottomLeftY <= height) & (yPosition - bottomLeftY >= 0)) {
                                output.collect(new Text(rNum), new Text(point));
                            }
                        }
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(SpatialJoin.class);
        conf.setMemoryForReduceTask(4096);
//        System.out.println(memoryLimit);
        conf.setJobName("SpatialJoin");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(Map.class);
//        conf.setCombinerClass(Reduce.class);
//        conf.setNumReduceTasks(1);
        conf.setReducerClass(Reduce.class);
//        conf.setReducerClass(Reduce2.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
//        System.out.println(args.length);
        if (args.length == 3) {
            window = args[2];
            System.out.println("window set");
//            System.out.println(wd.window);
        }

        JobClient.runJob(conf);
    }
}
