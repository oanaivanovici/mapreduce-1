package edu.rmit.cosc2633.s3759621.Assignment1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * WordCount class for Task 3 to count words using in-mapper combining.
 * Sources:
 * Lab 3, WordCount task, Task2code.txt
 * Lab 4, highestSalary task, HighestSalary.java class
 */

public class WordCount extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new WordCount(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "word count in-mapper combining");

        /* Run WordCount class from jar */
        job.setJarByClass(WordCount.class);

        /* Mapper and Reducer classes are WordCountMapper and GeneralReducer */
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(GeneralReducer.class);

        /* Output a (key, value) pair of (text, int) */
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        /* Input and output are passed from command line */
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
        return 0;
    }
}
