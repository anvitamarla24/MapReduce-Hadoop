//package edu.nyu.tandon.bigdata.hadoop;

import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/*** Custom Hadoop Record Reader : zipped file
 *
 * We want to produce (K,V) pairs where
 *    K = filename inside the zip file
 *    V = bytes corresponding to the file
 *
 * ***/
public class NYUZRecordReader extends RecordReader<Text, Text> {

    /** InputStream used to read the ZIP file from the FileSystem */
    private FSDataInputStream fsin;

    /** ZIP file parser/decompresser */
    private ZipInputStream zip;

    /** Uncompressed file name */
    private Text currentKey;

    /** Uncompressed file contents */
    private Text currentValue;

    /** Used to indicate progress */
    private boolean isFinished = false;

    /**
     * Initialise and open the ZIP file from the FileSystem
     */

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) inputSplit;
        Configuration conf = context.getConfiguration();
        Path path = split.getPath();
        FileSystem fs = path.getFileSystem(conf);

        // Open the stream
        fsin = fs.open(path);
        zip = new ZipInputStream(fsin);


    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        ZipEntry entry = null;
        try {
            entry = zip.getNextEntry();
        } catch (ZipException e) {
            if (NYUZInputFormat.getLenient() == false)
                throw e;
        }

        // Sanity check
        if (entry == null) {
            isFinished = true;
            return false;
        }

        // Filename
        currentKey = new Text(entry.getName());

        if (currentKey.toString().endsWith(".zip")) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            byte[] temp1 = new byte[8192];
            while (true) {
                int bytesread1 = 0;
                try {
                    bytesread1 = zip.read(temp1, 0, 8192);
                } catch (EOFException e) {
                    if (NYUZInputFormat.getLenient() == false)
                        throw e;
                    return false;
                }
                if (bytesread1 > 0)
                    bos.write(temp1, 0, bytesread1);
                else
                    break;
            }

            zip.closeEntry();
            currentValue = new Text(bos.toString());
            return true;

        }

        // Read the file contents
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] temp = new byte[8192];
        while (true) {
            int bytesRead = 0;
            try {
                bytesRead = zip.read(temp, 0, 8192);
            } catch (EOFException e) {
                if (NYUZInputFormat.getLenient() == false)
                    throw e;
                return false;
            }
            if (bytesRead > 0)
                bos.write(temp, 0, bytesRead);
            else
                break;
        }
        zip.closeEntry();

        // Uncompressed contents
        currentValue = new Text(bos.toString());
        return true;


/**
 * Rather than calculating progress, we just keep it simple
 */

    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return currentKey;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return currentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if(isFinished)
            return 1;
        else
            return 0;
    }

    @Override
    public void close() throws IOException {
        try {
            zip.close();
        } catch (Exception ignore) {
        }
        try {
            fsin.close();
        } catch (Exception ignore) {
        }
    }
}