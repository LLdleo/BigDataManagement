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


public class KMeansMapReduceExample {

    public static class Map extends Mapper<LongWritable, Text, IntWritable, Text>{

        //中心集合
        ArrayList<ArrayList<Double>> centers = null;
        //用k个中心
        int k = 0;

        //读取中心
        protected void setup(Context context) throws IOException,
                InterruptedException {
            centers = KMeansExample.getCentersFromHDFS(context.getConfiguration().get("centersPath"),false);
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
            ArrayList<Double> fields = KMeansExample.textToArray(value);
            int sizeOfFields = fields.size();

            double minDistance = 99999999;
            int centerIndex = 0;

            //依次取出k个中心点与当前读取的记录做计算
            for(int i=0; i<k; i++){
                double currentDistance = 0;
                for(int j=1;j<sizeOfFields;j++){
                    double centerPoint = Math.abs(centers.get(i).get(j));
                    double field = Math.abs(fields.get(j));
                    currentDistance += Math.pow((centerPoint - field) / (centerPoint + field), 2);//这里的距离算法简化处理
                }
                //循环找出距离该记录最接近的中心点的ID
                if(currentDistance<minDistance){
                    minDistance = currentDistance;
                    centerIndex = i;
                }
            }
            //以中心点为Key 将记录原样输出
            context.write(new IntWritable(centerIndex+1), value);
        }

    }

    //利用reduce的归并功能以中心为Key将记录归并到一起
    public static class Reduce extends Reducer<IntWritable, Text, Text, Text>{
        /**
         * 1.Key为聚类中心的ID value为该中心的记录集合
         * 2.计数所有记录元素的平均值，求出新的中心
         */
        protected void reduce(IntWritable key, Iterable<Text> value,Context context) throws IOException, InterruptedException {
            ArrayList<ArrayList<Double>> fieldsList = new ArrayList<>();

            //依次读取记录集，每行为一个ArrayList<Double>
            for (Text text : value) {
                ArrayList<Double> tempList = KMeansExample.textToArray(text);
                fieldsList.add(tempList);
            }

            //计算新的中心
            //每行的元素个数
            int filedSize = fieldsList.get(0).size();
            double[] avg = new double[filedSize];
            for(int i=1;i<filedSize;i++){
                //求没列的平均值
                double sum = 0;
                int size = fieldsList.size();
                for (ArrayList<Double> doubles : fieldsList) {
                    sum += doubles.get(i);
                }
                avg[i] = sum / size;
            }
            context.write(new Text("") , new Text(Arrays.toString(avg).replace("[", "").replace("]", "")));
        }

    }

    @SuppressWarnings("deprecation")
    public static void run(String centerPath,String dataPath,String newCenterPath,boolean runReduce) throws IOException, ClassNotFoundException, InterruptedException{

        Configuration conf = new Configuration();
        conf.set("centersPath", centerPath);

        Job job = new Job(conf, "mykmeans");
        job.setJarByClass(KMeansMapReduceExample.class);

        job.setMapperClass(Map.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        if(runReduce){
            //最后依次输出不需要reduce
            job.setReducerClass(Reduce.class);
            job.setOutputKeyClass(Text.class);
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
            if((KMeansExample.compareCenters(centerPath,newCenterPath)) | count >= 6){
                run(centerPath,dataPath,newCenterPath,false);
                break;
            }
        }
    }
}

class KMeansExample {

    //读取中心文件的数据
    static ArrayList<ArrayList<Double>> getCentersFromHDFS(String centersPath, boolean isDirectory) throws IOException{
        ArrayList<ArrayList<Double>> result = new ArrayList<>();
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
            ArrayList<Double> tempList = textToArray(line);
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
    static ArrayList<Double> textToArray(Text text){
        ArrayList<Double> list = new ArrayList<>();
        String[] fields = text.toString().split(",");
        for (String filed : fields) {
            list.add(Double.parseDouble(filed));
        }
        return list;
    }

    //比较新旧中心点的变化情况
    static boolean compareCenters(String centerPath, String newPath) throws IOException{

        List<ArrayList<Double>> oldCenters = KMeansExample.getCentersFromHDFS(centerPath,false);
        List<ArrayList<Double>> newCenters = KMeansExample.getCentersFromHDFS(newPath,true);

        int size = oldCenters.size();
        int fildSize = oldCenters.get(0).size();
        double distance = 0;
        for(int i=0;i<size;i++){
            for(int j=1;j<fildSize;j++){
                double t1 = Math.abs(oldCenters.get(i).get(j));
                double t2 = Math.abs(newCenters.get(i).get(j));
                distance += Math.pow((t1 - t2) / (t1 + t2), 2);  //根据具体情况选择相应的距离算法，这里简化处理
            }
        }

        if(distance == 0.0){
            //删掉新的中心文件以便最后依次归类输出
            KMeansExample.deletePath(newPath);
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
            KMeansExample.deletePath(newPath);
        }

        return false;
    }
}
