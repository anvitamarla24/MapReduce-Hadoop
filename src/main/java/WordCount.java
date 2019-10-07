import org.apache.commons.lang3.StringUtils;
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

public class WordCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        //private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private IntWritable result = new IntWritable();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            //QUESTION 2 - Printing the filename, line number and line.
            String lno = key.toString();
            String  l = value.toString();
            String str1 =  "door";


            String fileName = "\n" + ((FileSplit) context.getInputSplit()).getPath().getName();
            // referred - https://stackoverflow.com/questions/19012482/how-to-get-the-input-file-name-in-the-mapper-in-a-hadoop-program

            String[] lines = l.split("[!?.:]+"); //to get each line from all the lines
            for(String line : lines) {
                //line = line.replaceAll("\\s+"," "); //removing whitespaces
                line = line.replaceAll("[^a-zA-Z ]", ""); //removing punctuations
                line = line.replaceAll("\\s+"," ");
                if (StringUtils.containsIgnoreCase(line, "door")) {
                    //String[] lines = l.split("[!?.:]+");
                    String ll = line.toString();
                    context.write(new Text(fileName), new Text(lno + " " + ll));
                }//Q2 if
            }//Q2 for
                        /*
            //Question 3

            String filename = key.toString();

            // We only want to process .txt files
            if (filename.endsWith(".txt") == false)
                return;
            int count = 0;
            // Prepare the content
            String content = new String(value.getBytes(), "UTF-8");
            String[] lines = content.split("[!?.:]+");
            for(String line : lines) {
                line = line.replaceAll("\\s+"," "); //removing whitespaces
                line = line.replaceAll("[^a-zA-Z ]", ""); //removing punctuations
                count ++;
                if (StringUtils.containsIgnoreCase(line, "door")) {
                    //String[] lines = l.split("[!?.:]+");
                    String ll = line.toString();
                    context.write(new Text(filename), new Text(count + " " + ll));
                }//Q3 if
            }//Q3 for

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

        //Comment the following lines while executing Q2
        //job.setInputFormatClass(NYUZInputFormat.class);
        //job.setOutputKeyClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //Comment the following lines while executing Q2
        //NYUZInputFormat.setInputPaths(job, new Path(args[0]));
        //FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //Comment the following two lines for executing Q3 (line 107 and 108)
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}