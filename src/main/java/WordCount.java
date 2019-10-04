import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipException;
import java.util.zip.ZipInputStream;

import java.io.IOException;

//recordReader
class ZipFileRecordReader extends RecordReader<Text, Text> {
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
    public void initialize(InputSplit inputSplit,
                           TaskAttemptContext taskAttemptContext) throws IOException,
            InterruptedException {
        FileSplit split = (FileSplit) inputSplit;
        Configuration conf = taskAttemptContext.getConfiguration();
        Path path = split.getPath();
        FileSystem fs = path.getFileSystem(conf);

        // Open the stream
        fsin = fs.open(path);
        zip = new ZipInputStream(fsin);
    }

    /**
     * Each ZipEntry is decompressed and readied for the Mapper. The contents of
     * each file is held *in memory* in a BytesWritable object.
     *
     * If the ZipFileInputFormat has been set to Lenient (not the default),
     * certain exceptions will be gracefully ignored to prevent a larger job
     * from failing.
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        ZipEntry entry = null;
        try {
            entry = zip.getNextEntry();
        } catch (ZipException e) {
            if (ZipFileInputFormat.getLenient() == false)
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
                    if (ZipFileInputFormat.getLenient() == false)
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
                if (ZipFileInputFormat.getLenient() == false)
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
    }

    /**
     * Rather than calculating progress, we just keep it simple
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return isFinished ? 1 : 0;
    }

    /**
     * Returns the current key (name of the zipped file)
     */
    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return currentKey;
    }

    /**
     * Returns the current value (contents of the zipped file)
     * @return
     */
    @Override
    public Text getCurrentValue() throws IOException,
            InterruptedException {
        return currentValue;
    }

    /**
     * Close quietly, ignoring any exceptions
     */
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







public class WordCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        //private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private IntWritable result = new IntWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            //StringTokenizer itr = new StringTokenizer(value.toString());
            //String[] lines = itr.split("[!?.:]+");
            /*StringBuffer br=new StringBuffer();
            String token;
            while(itr.hasMoreTokens())
            {
                token=itr.nextToken();
                br.append(token.toUpperCase());
            }
            itr=null;
            word.set(br.toString());
            //context.write(NullWritable.get(), outvalue);
            br=null;*/


            //Question 3

            String fileName = key.toString();
            //LOG.info( "map: " + fileName );

            // We only want to process .txt files
            //String fileName = "\n" + ((FileSplit) context.getInputSplit()).getPath().getName();
            if (fileName.endsWith(".txt") == false)
                return;

            String content = new String(value.getBytes(), "UTF-8");

            //Question 2

            //String lno = key.toString();
            String  l = value.toString();
            String str1 =  "door";


            //String fileName = "\n" + ((FileSplit) context.getInputSplit()).getPath().getName();
            // refereed - https://stackoverflow.com/questions/19012482/how-to-get-the-input-file-name-in-the-mapper-in-a-hadoop-program


            /*while (itr.hasMoreTokens()) {
                //word.set(itr.nextToken());
                String str1 = "door";
                String str2;
                str2 = itr.nextToken();
                if(str2.equalsIgnoreCase(str1)) {

                    //String[] lines = itr.split("[!?.:]+");

                    word.set(str2);
                    //context.write((Text) key, result);
                    context.write(word, new Text(lno + " " + l));
                }

            }*/

            //if(l.equalsIgnoreCase(str1)) {
            //if(StringUtils.equalsIgnoreCase(l,"door")){
                //context.write(new Text(fileName) ,new IntWritable( Integer.parseInt(lno + " " + l)));
            //}
            String[] lines = l.split("[!?.:]+");
            for(String line : lines) {
                if (StringUtils.containsIgnoreCase(line, "door")) {
                    //String[] lines = l.split("[!?.:]+");
                    String ll = line.toString();
                    context.write(new Text(fileName), new Text(ll));
                }
            }

        }
    }

    public static class IntSumReducer
            extends Reducer<Text, Text, Text, Text> {
        //private IntWritable result = new IntWritable();
        //private final static IntWritable value = new IntWritable();
        //private Text word = new Text();
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            /*int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);*/
            //IntWritable value = new IntWritable(Integer.parseInt(values));
            for(Text value : values){
                //result = val.get();
                context.write(key,value);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        // this is an example comment

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCount.TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        //job.setInputFormatClass(ZipFileInputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);

        //ZipFileInputFormat.setLenient( true );
        //ZipFileInputFormat.setInputPaths(job, new Path(args[0]));
        //TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(ZipFileInputFormat.class);
        job.setOutputKeyClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        ZipFileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        /*job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));*/
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
