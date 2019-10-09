//referred - https://stackoverflow.com/questions/32714295/hadoop-decompressed-zip-files
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
public class NYUZInputFormat extends FileInputFormat<Text, Text> {
    private static boolean isLenient = false;

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    public RecordReader<Text, Text> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException {
        return new NYUZRecordReader();
    }

    public static void setLenient(boolean lenient) {
        isLenient = lenient;
    }

    public static boolean getLenient() {
        return isLenient;
    }
}