import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class KMeansMapReduce {



    public static class Map extends Mapper<LongWritable, Text, IntWritable, Text>{

        //中心集合
        ArrayList<ArrayList<String>> centers = null;
        //用k个中心
        int k = 0;

        //读取中心
        protected void setup(Context context) throws IOException,
                InterruptedException {
            centers = KMeans.getCentersFromHDFS(context.getConfiguration().get("centersPath"),false);
            k = centers.size();//获取中心点个数
            System.out.println(centers);
            System.out.println("总共有" + k + "个中心点");
        }

        /**
         * 1.每次读取一条要分类的条记录与中心做对比，归类到对应的中心
         * 2.以中心ID为key，中心包含的记录为value输出(例如： 1 0.2---->1为聚类中心的ID，0.2为靠近聚类中心的某个值)
         */
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //读取一行数据
            String point = value.toString();

            double minDistance = 15000;
            double currentDistance = 0;
            int centerIndex = 0;

            //依次取出k个中心点与当前读取的记录做计算
            for(int i=0; i<k; i++){
                String centerPoint = centers.get(i).get(1);
                currentDistance = distance(centerPoint, point);//这里的距离算法简化处理
                //循环找出距离该记录最接近的中心点的ID
                if(currentDistance<minDistance){
                    minDistance = currentDistance;
                    centerIndex = i;
                }
            }
            //以中心点为Key 将记录原样输出
            context.write(new IntWritable(centerIndex+1), value);
        }

        double distance(String point1, String point2) {
            String[] p1 = point1.split(",");
            String[] p2 = point2.split(",");
            double p1x = Double.parseDouble(p1[0]);
            double p1y = Double.parseDouble(p1[1]);
            double p2x = Double.parseDouble(p2[0]);
            double p2y = Double.parseDouble(p2[0]);
            return Math.sqrt(Math.pow(p1x - p2x, 2) + Math.pow(p1y - p2y, 2));
        }

    }

    //利用reduce的归并功能以中心为Key将记录归并到一起
    public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text>{
        /**
         * 1.Key为聚类中心的ID value为该中心的记录集合
         * 2.计数所有记录元素的平均值，求出新的中心
         */
        protected void reduce(IntWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
            ArrayList<String> pointsList = new ArrayList<>();

            //依次读取记录集，每行为一个ArrayList<Double>
            for (Text value : values) {
                pointsList.add(value.toString());
            }

            int valueNum = pointsList.size();
            int sumX = 0;
            int sumY = 0;

            for (String point : pointsList) {
                String[] pointP = point.split(",");
                int px = Integer.parseInt(pointP[0]);
                int py = Integer.parseInt(pointP[1]);
                sumX += px;
                sumY += py;
            }

            double avgX = (double) sumX/valueNum;
            double avgY = (double) sumY/valueNum;
            context.write(key , new Text(avgX + "," + avgY));
        }
    }

    @SuppressWarnings("deprecation")
    public static void run(String centerPath,String dataPath,String newCenterPath,boolean runReduce) throws IOException, ClassNotFoundException, InterruptedException{

        Configuration conf = new Configuration();
        conf.set("centersPath", centerPath);
        // 设置分隔符
        conf.set("mapred.textoutputformat.ignoreseparator","true");
        conf.set("mapred.textoutputformat.separator", "#");

        Job job = new Job(conf, "mykmeans");
        job.setJarByClass(KMeansMapReduce.class);

        job.setMapperClass(Map.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        if(runReduce){
            //最后依次输出不需要reduce
            job.setReducerClass(Reduce.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
        }

        FileInputFormat.addInputPath(job, new Path(dataPath));
        FileOutputFormat.setOutputPath(job, new Path(newCenterPath));

        System.out.println(job.waitForCompletion(true));
    }

    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
        String centerPath = "fileInput/Centers";  //存放中心点坐标值
        String dataPath = "fileInput/PointsTest";  //存放待处理数据
        String newCenterPath = "fileOutput/Centers";  //结果输出目录

        int count = 0;

        while(true){
            run(centerPath,dataPath,newCenterPath,true);
            System.out.println(" 第 " + ++count + " 次计算 ");
            if ((KMeans.compareCenters(centerPath,newCenterPath)) | count >= 6){
                run(centerPath,dataPath,newCenterPath,false);
                break;
            }
        }
    }
}

class KMeans {

    //读取中心文件的数据
    static ArrayList<ArrayList<String>> getCentersFromHDFS(String centersPath, boolean isDirectory) throws IOException{
        ArrayList<ArrayList<String>> result = new ArrayList<>();
        Path path = new Path(centersPath);
        Configuration conf = new Configuration();

        FileSystem fileSystem = path.getFileSystem(conf);

        if(isDirectory){
            FileStatus[] listFile = fileSystem.listStatus(path);
            for (FileStatus fileStatus : listFile) {
                result.addAll(getCentersFromHDFS(fileStatus.getPath().toString(), false));
            }
            return result;
        }

        FSDataInputStream FSDIS = fileSystem.open(path);
        LineReader lineReader = new LineReader(FSDIS, conf);

        Text line = new Text();

        while(lineReader.readLine(line) > 0){
            ArrayList<String> tempList = textToArray(line);
            result.add(tempList);
        }
        lineReader.close();
        System.out.println(result);
        return result;
    }

    //删掉文件
    private static void deletePath(String pathStr) throws IOException{
        Configuration conf = new Configuration();
        Path path = new Path(pathStr);
        FileSystem hdfs = path.getFileSystem(conf);
        hdfs.delete(path ,true);
    }

    //文本string类型转为数组类型
    private static ArrayList<String> textToArray(Text text){
        String[] fields = text.toString().split("#");
        return new ArrayList<>(Arrays.asList(fields));
    }

    static double distance(String point1, String point2) {
        String[] p1 = point1.split(",");
        String[] p2 = point2.split(",");
        double p1x = Double.parseDouble(p1[0]);
        double p1y = Double.parseDouble(p1[1]);
        double p2x = Double.parseDouble(p2[0]);
        double p2y = Double.parseDouble(p2[0]);
        return Math.sqrt(Math.pow(p1x - p2x, 2) + Math.pow(p1y - p2y, 2));
    }

    //比较新旧中心点的变化情况
    static boolean compareCenters(String centerPath, String newPath) throws IOException{

        List<ArrayList<String>> oldCenters = KMeans.getCentersFromHDFS(centerPath,false);
        List<ArrayList<String>> newCenters = KMeans.getCentersFromHDFS(newPath,true);

        int size = oldCenters.size();
        double distance = 0;
        for(int i=0;i<size;i++){
            distance = distance(oldCenters.get(i).get(1), newCenters.get(i).get(1));
        }

        if(distance == 0.0){
            //删掉新的中心文件以便最后依次归类输出
            KMeans.deletePath(newPath);
            return true;
        }else{
            //先清空中心文件，将新的中心文件复制到中心文件中，再删掉中心文件

            Configuration conf = new Configuration();
            Path outPath = new Path(centerPath);
            FileSystem fileSystem = outPath.getFileSystem(conf);

            FSDataOutputStream overWrite = fileSystem.create(outPath,true);
            overWrite.writeChars("");
            overWrite.close();


            Path inPath = new Path(newPath);
            FileStatus[] listFiles = fileSystem.listStatus(inPath);
            for (FileStatus listFile : listFiles) {
                FSDataOutputStream out = fileSystem.create(outPath);
                FSDataInputStream in = fileSystem.open(listFile.getPath());
                IOUtils.copyBytes(in, out, 4096, true);
            }
            //删掉新的中心文件以便第二次任务运行输出
            KMeans.deletePath(newPath);
        }

        return false;
    }
}
