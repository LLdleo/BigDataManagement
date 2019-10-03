import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;


public class JsonFileInputFormat extends FileInputFormat {

//    @Override
//    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
//        return new JsonLineRecordReader();
//    }

    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        JsonLineRecordReader reader = new JsonLineRecordReader();
        reader.initialize(split, context);
        return reader;

    }

//    @Override
//    public List<InputSplit> getSplits(JobContext job) throws IOException {
//
//        int numSplit = 5;
//        int numLineOneSplit = 15;
//        int numLines;
//        Configuration conf = job.getConfiguration();
//        List<InputSplit> splits = new ArrayList<InputSplit>();
//        for (FileStatus status : listStatus(job)) {
//            Path fileName = status.getPath();
//            FileSystem fs = fileName.getFileSystem(conf);
//            LineReader lr = null;
//            FSDataInputStream in = fs.open(fileName);
//            lr = new LineReader(in, conf);
//            numLines = 0;
//            while (lr.readLine(new Text()) > 0) {
//                numLines++;
//            }
//            int numOfBlock = numLines / numLineOneSplit / numSplit + 1;
//            int length = numLineOneSplit * numOfBlock;
//            splits.addAll(NLineInputFormat.getSplitsForFile(status, conf, length));
//        }
//
//        return splits;
//    }
}

class JsonLineRecordReader extends RecordReader<String, String> {

    private long start;
    private long pos;
    private long end;
    private LineRecordReader lineRecordReader;
//    private int maxLineLength;
    private String key;
    private String value;
    private int jsonOrder =0;
    private int order =0;

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

        String[] info = new String[13];

        if (!lineRecordReader.nextKeyValue()) {
            // We've reached end of Split
            key = null;
            value = null;
            return false;
        }
        Text row = lineRecordReader.getCurrentValue();

        if (row.toString().contains("}")) {
            key = "" + jsonOrder;
            value = StringUtils.join(",", info);
            jsonOrder++;
            order = 0;
            return true;
        }

        // json start
        if (!row.toString().contains("{")) {
            info[order] = row.toString().split(":")[1].trim().replace("\"","").replace(",","");
            order++;
        }
        return nextKeyValue();
    }

    /**
     * This methods are used by the framework to give generated key/value pairs
     * to an implementation of Mapper. Be sure to reuse the objects returned by
     * these methods if at all possible!
     */
    @Override
    public String getCurrentKey() throws IOException,
            InterruptedException {
        return key;
    }

    /**
     * This methods are used by the framework to give generated key/value pairs
     * to an implementation of Mapper. Be sure to reuse the objects returned by
     * these methods if at all possible!
     */
    @Override
    public String getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    /**
     * Like the corresponding method of the InputFormat class, this is an
     * optional method used by the framework for metrics gathering.
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / (float) (end - start));
        }
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