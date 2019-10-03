import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CustomInputFormat {
    public static class JSONInputFormat extends FileInputFormat<Text, Text> {

        public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws
                IOException, InterruptedException {

            JSONRecordReader reader = new JSONRecordReader();
            reader.initialize(split, context);
            return reader;

        }

        @Override
        public List<InputSplit> getSplits(JobContext job) throws IOException {

            int numSplit = 5;
            int numLineOneSplit = 15;
            int numLines;
            Configuration conf = job.getConfiguration();
            List<InputSplit> splits = new ArrayList<InputSplit>();
            for (FileStatus status : listStatus(job)) {
                Path fileName = status.getPath();
                FileSystem fs = fileName.getFileSystem(conf);
                LineReader lr = null;
                FSDataInputStream in = fs.open(fileName);
                lr = new LineReader(in, conf);
                numLines = 0;
                while (lr.readLine(new Text()) > 0) {
                    numLines++;
                }
                int numOfBlock = numLines / numLineOneSplit / numSplit + 1;
                int length = numLineOneSplit * numOfBlock;
                splits.addAll(NLineInputFormat.getSplitsForFile(status, conf, length));
            }

            return splits;
        }
    }

    public static class JSONRecordReader extends RecordReader<Text, Text> {
        int count = 0;
        String temp_value = "";
        LineRecordReader lineRecordReader = null;
        Text key;
        Text value;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            close();
            lineRecordReader = new LineRecordReader();
            lineRecordReader.initialize(split, context);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {

            if (!lineRecordReader.nextKeyValue()) {
                key = null;
                value = null;
                return false;
            }

            Text line = lineRecordReader.getCurrentValue();
            String str = line.toString();

            if (str.contains("}") || str.contains("},")) {
                key = new Text(Integer.toString(count));
                value = new Text(temp_value.replaceAll("\"", ""));
                count++;
                temp_value = "";
                return true;
            }

            String arr = str.replaceAll("\\s+", "");
            if (!arr.equals("{")) {
                temp_value += arr;
            }
            return nextKeyValue();
        }

        @Override
        public void close() throws IOException {
            if (null != lineRecordReader) {
                lineRecordReader.close();
                lineRecordReader = null;
            }
            key = null;
            value = null;
        }

        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return lineRecordReader.getProgress();
        }
    }

    public static class MyMapper extends Mapper<Text, Text, Text, Text> {

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] l = value.toString().split(",");
            context.write(new Text(l[5].split(":")[1]), new Text(l[8].split(":")[1]));

        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> value, Context context) throws IOException,
                InterruptedException {
            int count = 0;
            int maxElevation = 0;
            int minEelvation = 100000;

            for (Text v : value) {
                String l = v.toString();
                if(Integer.parseInt(l) > maxElevation){
                    maxElevation = Integer.parseInt(l);
                }
                if(Integer.parseInt(l) < minEelvation){
                    minEelvation = Integer.parseInt(l);
                }
                count++;
            }

            context.write(key, new Text(count + "," + maxElevation + "," + minEelvation));
            // key is "Flags", value is "the number of records in each key, maximum elevation values in each flag, miminum elevation values in each flag
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "CustomInput");
        job.setJarByClass(CustomInputFormat.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(JSONInputFormat.class); // use the class we create
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
