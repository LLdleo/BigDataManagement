import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FlagAggregate {
    public static class JsonFileInputFormat extends FileInputFormat {
        public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            JsonLineRecordReader reader = new JsonLineRecordReader();
            reader.initialize(split, context);
            return reader;

        }
    }

    public static class JsonLineRecordReader extends RecordReader<Text, Text> {

        private LineRecordReader lineRecordReader;
        //    private int maxLineLength;
        private Text key = new Text();
        private Text value = new Text();
        private int jsonOrder =0;
        private int order =0;
        private List<String> info = new ArrayList<>();

        /**
         * This method takes as arguments the map task’s assigned InputSplit and
         * TaskAttemptContext, and prepares the record reader. For file-based input
         * formats, this is a good place to seek to the byte position in the file to
         * begin reading.
         */
        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            close();
            lineRecordReader = new LineRecordReader();
            lineRecordReader.initialize(split, context);
        }

        /**
         * Like the corresponding method of the InputFormat class, this reads a
         * single key/ value pair and returns true until the data is consumed.
         */
        @Override
        public boolean nextKeyValue() throws IOException {
            // Current offset is the key

            key = new Text();
            value = new Text();

            if (!lineRecordReader.nextKeyValue()) {
                // We've reached end of Split
                key = null;
                value = null;
                return false;
            }
            Text row = lineRecordReader.getCurrentValue();
//            System.out.println(order + row.toString());

            if (row.toString().contains("}")) {
                key.set(String.valueOf(jsonOrder));
//                System.out.println(info);
                String wholeString = StringUtils.join(",", info);
//                System.out.println(wholeString);
                value.set(wholeString);
                info = new ArrayList<>();
                jsonOrder++;
                order = 0;
                return true;
            }

            // json start
            if (!row.toString().contains("{")) {
                String temp = row.toString().split(":")[1].trim().replace("\"","").replace(",","");
                info.add(temp);
//                System.out.println(temp);
                order++;
//                System.out.println(info[order-1]);
            }
            return nextKeyValue();
        }

        /**
         * This methods are used by the framework to give generated key/value pairs
         * to an implementation of Mapper. Be sure to reuse the objects returned by
         * these methods if at all possible!
         */
        @Override
        public Text getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        /**
         * This methods are used by the framework to give generated key/value pairs
         * to an implementation of Mapper. Be sure to reuse the objects returned by
         * these methods if at all possible!
         */
        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        /**
         * Like the corresponding method of the InputFormat class, this is an
         * optional method used by the framework for metrics gathering.
         */
        @Override
        public float getProgress() throws IOException, InterruptedException {
            return lineRecordReader.getProgress();
        }

        /**
         * This method is used by the framework for cleanup after there are no more
         * key/value pairs to process.
         */
        @Override
        public void close() throws IOException {
            if (null != lineRecordReader) {
                lineRecordReader.close();
                lineRecordReader = null;
            }
            key = null;
            value = null;
        }
    }
    public static class Map extends Mapper<Text, Text, IntWritable, IntWritable> {

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] valueSplit = value.toString().split(",");
            int flag = Integer.parseInt(valueSplit[5]);
            int evaluation = Integer.parseInt(valueSplit[8]);
            context.write(new IntWritable(flag), new IntWritable(evaluation));
        }
    }

    public static class Reduce extends Reducer<IntWritable,IntWritable,IntWritable,Text> {

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            List<Integer> evaluationList = new ArrayList<>();

            for (IntWritable evaluation : values) {
                evaluationList.add(evaluation.get());
            }

            int maxEvaluation = 0;
            int minEvaluation = 100;

            for (int evaluation : evaluationList) {
                if (evaluation > maxEvaluation) {
                    maxEvaluation = evaluation;
                }
                if (evaluation < minEvaluation) {
                    minEvaluation = evaluation;
                }
            }

            context.write(key, new Text("max: " + maxEvaluation + ", min: " + minEvaluation));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "FlagAggregate");
        job.setJarByClass(FlagAggregate.class);
        job.setMapperClass(Map.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(JsonFileInputFormat.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.out.println(job.waitForCompletion(true) ? 0 : 1);
    }
}
