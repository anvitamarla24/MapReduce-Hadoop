import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

//inputformat - zip
public class ZipFileInputFormat extends FileInputFormat<Text, Text> {
    /** See the comments on the setLenient() method */
    private static boolean isLenient = false;

    /**
     * ZIP files are not splitable so they cannot be overrided so function
     * return false
     */
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    /**
     * Create the ZipFileRecordReader to parse the file
     */
    @Override
    public RecordReader<Text, Text> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException {
        return new ZipFileRecordReader();
    }

    /**
     *
     * @param lenient
     */
    public static void setLenient(boolean lenient) {
        isLenient = lenient;
    }

    public static boolean getLenient() {
        return isLenient;
    }
}
