import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WordCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        //private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private IntWritable result = new IntWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            //QUESTION 2 - Printing the filename, line number and line where input is a text file.
            //COMMENT THE FOLLOWING BLOCK OF CODE WHILE EXECUTING Q3
            String lno = key.toString();
            String  l = value.toString();
            String fileName = "\n" + ((FileSplit) context.getInputSplit()).getPath().getName();
            // referred - https://stackoverflow.com/questions/19012482/how-to-get-the-input-file-name-in-the-mapper-in-a-hadoop-program

            String[] lines = l.split("[!?.:]+"); //to get each line from all the lines
            for(String line : lines) {
                line = line.replaceAll("[^a-zA-Z ]", ""); //removing punctuations
                line = line.replaceAll("\\s+"," "); //removing whitespaces
                Pattern pattern = Pattern.compile("\\bdoor\\b", Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(line);
                while (matcher.find()){
                    String ll = line.toString();
                    context.write(new Text(fileName), new Text(lno + " " + ll));
                }
            }

            /*
            //Question 3 - Print the filename, line number and line where input is a zip file.
            //COMMENT THE FOLLOWING BLOCK OF CODE WHILE EXECUTING Q2
            //referred - https://stackoverflow.com/questions/32714295/hadoop-decompressed-zip-files

            String filename = key.toString();

            // We only want to process .txt files
            if (filename.endsWith(".txt") == false)
                return;
            int count = 0;
            // Prepare the content
            String content = new String(value.getBytes(), "UTF-8");
            String[] lines = content.split("[!?.:]+");
            for(String line : lines) {
                count ++;
                line = line.replaceAll("[^a-zA-Z ]", ""); //removing punctuations
                line = line.replaceAll("\\s+"," "); //removing whitespaces
                Pattern pattern = Pattern.compile("\\bdoor\\b", Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(line);
                while (matcher.find()){
                    String ll = line.toString();
                    context.write(new Text(filename), new Text(count + " " + ll));

                }//Q3 while ends
            }//Q3 for ends
            */
        }
    }

    public static class IntSumReducer
            extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
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

        //Comment the following two lines while executing Q2 and uncomment them while executing Q3
        //job.setInputFormatClass(NYUZInputFormat.class);
        //job.setOutputKeyClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //Comment the following two lines while executing Q2 and uncomment them while executing Q3
        //NYUZInputFormat.setInputPaths(job, new Path(args[0]));
        //FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //Comment the following two lines while executing Q3 and uncomment them while executing Q2
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}