import java.io.BufferedReader;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
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

public class WordCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
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

            String lno = key.toString();
            String  l = value.toString();
            String str1 =  "door";


            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
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
            if(StringUtils.equalsIgnoreCase(l,"door")){
                context.write(new Text(fileName) ,new IntWritable( Integer.parseInt(lno + " " + l)));
            }

        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        //private IntWritable result = new IntWritable();
        private final static IntWritable value = new IntWritable();
        //private Text word = new Text();
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            /*int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);*/
            //IntWritable value = new IntWritable(Integer.parseInt(values));
            for( IntWritable value : values){
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
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
