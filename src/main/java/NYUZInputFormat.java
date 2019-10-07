//package edu.nyu.tandon.bigdata.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Extends the basic FileInputFormat to accept ZIP files.
 * ZIP files are not 'splittable', so we need to process/decompress in place:
 * each ZIP file will be processed by a single Mapper; we are parallelizing files, not lines...
 */
public class NYUZInputFormat extends FileInputFormat<Text, Text> {
    private static boolean isLenient = false;
/**
 * ZIP files are not splitable so they cannot be overrided so function
 * return false
 */
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }


    /*** return a record reader
     *
     * @param split
     * @param context
     * @return (Text,BytesWritable)
     * @throws IOException
     * @throws InterruptedException
     */
    public RecordReader<Text, Text> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException {
        return new NYUZRecordReader();
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