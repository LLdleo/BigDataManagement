import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
//这种方法是小文件加载，匹配大文件，不需要reduce，所有将reducetask关掉

public class Query2 {
    static class MyMapper extends Mapper<Object, Text, Text, Text>{
/*        IntWritable mk=new IntWritable();
        Text mv=new Text();
        Map<Integer,String> map=new HashMap<Integer, String>();
        private URI[] files;
        //读取movies这个表  将表中的所有数据先加载过来
        @Override*/

        private HashMap city_info = new HashMap();
        private Text outPutKey = new Text();
        private Text outPutValue = new Text();
        private String mapInputStr = null;
        private String mapInputSpit[] = null;
        private Object city_secondPart = null;
        /**
         * 此方法在每一个task開始之前运行，这里主要用作从DistributedCache
         * 中取到tb_dim_city文件。并将里边记录取出放到内存中。

         */

        protected void setup(Context context)
                throws IOException, InterruptedException {
            BufferedReader br = null;
            //获得当前作业的DistributedCache相关文件
            Path[] distributePaths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            String cityInfo = null;
            for(Path p : distributePaths){
                if(p.toString().endsWith("customer.txt")){
                    //读缓存文件，并放到mem中
                    br = new BufferedReader(new FileReader(p.toString()));
                    while(null!=(cityInfo=br.readLine())){
                        String[] cityPart = cityInfo.split(",");

                        city_info.put(cityPart[0], cityPart[1]);

                    }
                }
            }
        }

/*        protected void setup(Context context)
                throws IOException, InterruptedException {
            //进行movies文件的读取
            files = DistributedCache.getCacheFiles(context.getConfiguration());
            Path path = new Path(files[0]);
            //创建一个字符流
            BufferedReader br=new BufferedReader(new FileReader(path.toString()));
            String line=null;
            System.out.println("test");
            while((line=br.readLine())!=null){
                //1::Toy Story (1995)::Animation|Children's|Comedy
                String[] split = line.split(",");
                map.put(Integer.parseInt(split[0]), split[1]);
            }

        }*/

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            //先读取ratings文件  每次读取一行   和map集合中的数据进行关联
            // 1::1193::5::978300760   ratings.dat
            mapInputStr = value.toString();
            mapInputSpit = mapInputStr.split(",");
            city_secondPart = city_info.get(mapInputSpit[1]);
            this.outPutKey.set(mapInputSpit[1]);
            this.outPutValue.set(city_secondPart+"\t"+mapInputSpit[2]);
            context.write(outPutKey, outPutValue);


            //String[] split = value.toString().split(",");
            //String k=split[1];
/*            if(map.containsKey(Integer.parseInt(k))){
                //进行关联  取出map的value  和   现在的数据进行关联
                String res=map.get(Integer.parseInt(k))+"\t"+split[2];
                mk.set(Integer.parseInt(res));
                mv.set(res);
                context.write(mk, mv);
            }*/
        }
    }

/*    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }*/

    public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        //加载配置文件
        Configuration conf=new Configuration();
        //启动一个job  这里的mr任务叫做job   这个job作用  封装mr任务
        Job job=new Job(conf,"Distributed Cache");

        //指定当前任务的主类   jarclass=Driver.class
        job.setJarByClass(Query2.class);

        //指定map
        job.setMapperClass(MyMapper.class);

        /*
         * 泛型：jdk1.5  泛型的编译的时候生效  检查代码的类型是否匹配  运行的时候自动擦除
         */
        //指定map输出的key  value的类型
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        //将指定的文件加载到每一个运行计算任务节点的缓存中
        DistributedCache.addCacheFile(new URI("/Users/daojun/Desktop/mapreduce/input/customer.txt"), conf);
        //不需要reducetask设置这里
        job.setNumReduceTasks(0);
        //修改切片的大小
        FileInputFormat.addInputPath(job,new Path(args[0]));
        //指定输出目录  输出路径不能存在  否则报错  代码运行的饿时候  会帮你创建
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //提交job  参数：是否打印执行日志
        job.waitForCompletion(true);
    }
}


