import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

public class JsonInputFormat extends FileInputFormat<Text, Text> {
//    public static final String CONFIG_MEMBER_NAME = "jsoninputformat.member";

    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        return new JsonReader();
    }

}

class JsonReader extends RecordReader<Text ,Text>{
    //  private BufferedReader in;
    private LineReader lr ;
    private Text key = new Text();
    private Text value = new Text();
    private long start ;
    private long end;
    private long currentPos;
    private Text line = new Text();
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext cxt) throws IOException, InterruptedException {
        FileSplit split =(FileSplit) inputSplit;
        Configuration conf = cxt.getConfiguration();
        Path path = split.getPath();
        FileSystem fs = path.getFileSystem(conf);
        FSDataInputStream is = fs.open(path);
        lr = new LineReader(is,conf);

        // 处理起始点和终止点
        start =split.getStart();
        end = start + split.getLength();
        is.seek(start);
        if(start!=0){
            start += lr.readLine(new Text(),0, (int)Math.min(Integer.MAX_VALUE, end-start));
        }
        currentPos = start;
    }

    // 针对每行数据进行处理
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(currentPos > end){
            return false;
        }
        currentPos += lr.readLine(line);
        if(line.getLength()==0){
            return false;
        }
        if(line.toString().startsWith("ignore")){
            currentPos += lr.readLine(line);
        }

        String [] words = line.toString().split(",");
        // 异常处理
        if(words.length<2){
            System.err.println("line:"+line.toString()+".");
            return false;
        }
        key.set(words[0]);
        value.set(words[1]);
        return true;

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
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (currentPos - start) / (float) (end - start));
        }
    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub
        lr.close();
    }

}
