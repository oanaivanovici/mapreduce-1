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
 * WordFirstChar class for Task 2 to count words based on whether the first character is a
 * vowel or consonant.
 * Sources:
 * Lab 3, WordCount task, Task2code.txt
 * Lab 4, highestSalary task, HighestSalary.java class
 */

public class WordFirstChar extends Configured implements Tool {

    public static void main(String[] args) throws Exception  {
        System.exit(ToolRunner.run(new WordFirstChar(), args));
    }


    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "word first character");

        /* Run WordFirstChar class from jar */
        job.setJarByClass(WordFirstChar.class);

        /* Mapper and Reducer classes are WordFirstCharMapper and GeneralReducer */
        job.setMapperClass(WordFirstCharMapper.class);
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
