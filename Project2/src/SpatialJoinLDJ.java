import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SpatialJoinLDJ {

    public static class pointMapper
            extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String window = conf.get("window");
            String[] w = window.split(",");
            int windowX1 = Integer.parseInt(w[0]);
            int windowY1 = Integer.parseInt(w[1]);
            int windowX2 = Integer.parseInt(w[2]);
            int windowY2 = Integer.parseInt(w[3]);
            String line = value.toString();
            String[] lines = line.split(",");
            int x = Integer.parseInt(lines[0]);
            int y = Integer.parseInt(lines[1]);
            if(x >= windowX1 && x <= windowX2 && y >= windowY1 && y <= windowY2){
                context.write(new Text("1"), new Text("point"+ "," + line));
            }
        }
    }

    public static class recMapper
            extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String window = conf.get("window");
            String[] w = window.split(",");
            int windowX1 = Integer.parseInt(w[0]);
            int windowY1 = Integer.parseInt(w[1]);
            int windowX2 = Integer.parseInt(w[2]);
            int windowY2 = Integer.parseInt(w[3]);
            String line = value.toString();
            String[] lines = line.split(",");
            int x = Integer.parseInt(lines[0]);
            int y = Integer.parseInt(lines[1]);
            int height = Integer.parseInt(lines[2]);
            int width = Integer.parseInt(lines[3]);
            if(x >= windowX1 && x <= windowX2 && y >= windowY1 && y <= windowY2){
                context.write(new Text("1"), new Text("rec"+ "," + line));
            }
            else if((x+width) >= windowX1 && (x+width) <= windowX2 && y >= windowY1 && y <= windowY2){
                context.write(new Text("1"), new Text("rec"+ "," + line));
            }
            else if(x >= windowX1 && x <= windowX2 && (y+height) >= windowY1 && (y+height) <= windowY2){
                context.write(new Text("1"), new Text("rec"+ "," + line));
            }
            else if((x+width) >= windowX1 && (x+width) <= windowX2 && (y+height) >= windowY1 && (y+height) <= windowY2){
                context.write(new Text("1"), new Text("rec"+ "," + line));
            }

        }
    }

    public static class randPReducer
            extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> value, Context context
        ) throws IOException, InterruptedException {

            List<String> p = new ArrayList<String>();
            List<String> r = new ArrayList<String>();
            int recBottomX;
            int recBottomY;
            int recH;
            int recW;
            int pointX;
            int pointY;

            for (Text v:value) {
                String[] l = v.toString().split(",");

                if (l[0].equals("point")){
                    p.add(l[1] + "," + l[2]);
                }
                else if (l[0].equals("rec")) {
                    r.add(l[1] + "," + l[2] + "," + l[3] + "," + l[4]);
                }
            }

            for(String rec: r){
                String[] recList = rec.split(",");
                recBottomX = Integer.parseInt(recList[0]);
                recBottomY = Integer.parseInt(recList[1]);
                recH = Integer.parseInt(recList[2]);
                recW = Integer.parseInt(recList[3]);
                for(String point: p){
                    String[] pointList = point.split(",");
                    pointX = Integer.parseInt(pointList[0]);
                    pointY = Integer.parseInt(pointList[0]);

                    if(pointX >= recBottomX && pointX <= (recBottomX + recW) && pointY >= recBottomY && pointY <= (recBottomY + recH)){
                        context.write(new Text(rec), new Text(point));
                    }

                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("window", "5,20,100,100");
        Job job = new Job(conf, "pandRSpatialJoin");
        job.setJarByClass(SpatialJoinLDJ.class);
        job.setOutputKeyClass(Text.class);
        job.setReducerClass(SpatialJoinLDJ.randPReducer.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class, pointMapper.class);
        MultipleInputs.addInputPath(job,new Path(args[1]), TextInputFormat.class, recMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
