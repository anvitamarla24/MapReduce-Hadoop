# MyFirstRepo

ASSIGNMENT 1:
Anvita Marla
N19407076
am9435

The three programs for Q2 and Q3 are in the IdeaProjects -> MyFirstRepo -> src -> main -> java 

WordCount.java has Q2 and Q3 code.
NYUZInputFormat.java has the ZipInputFormat code required for Q3.
NYUZRecordReader.java has the RecordReaded code required for Q3.

I have done both Q2 and Q3 in the same WordCount.java program.
Additionally, I have also included comments for better understanding what part of the code does what.

STEPS TO EXECUTE QUESTION 2:

By default, I have submitted the code for Q2 and commenting the code for Q3.
If it wasn't the case, 
PLEASE comment from line 53: 
String filename = key.toString();

            // We only want to process .txt files
            if (filename.endsWith(".txt") == false)
.... 
to line 73 :  context.write(new Text(filename), new Text(count + " " + ll));

                }//Q3 while ends
            }//Q3 for ends

Also comment lines 102 and 103:

job.setInputFormatClass(NYUZInputFormat.class);
job.setOutputKeyClass(TextOutputFormat.class);

Additionally also comment lines 109 and 110:

NYUZInputFormat.setInputPaths(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));


STEPS TO EXECUTE QUESTION 3:

PLEASE comment out the block of code from line 31:
String lno = key.toString();
            String  l = value.toString();
....
to line 46:
context.write(new Text(fileName), new Text(lno + " " + ll));
                }//Q2 while ends
            }//Q2 for ends

Also UNCOMMENT lines 102 and 103 which was commented while executing Q2:

job.setInputFormatClass(NYUZInputFormat.class);
job.setOutputKeyClass(TextOutputFormat.class);

Also UNCOMMENT lines 109 and 110 which was commented while executing Q2:

NYUZInputFormat.setInputPaths(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));

Additionally, COMMENT the lines 113 and 114:

FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));




