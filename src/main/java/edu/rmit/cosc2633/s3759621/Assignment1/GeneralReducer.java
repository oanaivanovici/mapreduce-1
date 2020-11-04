package edu.rmit.cosc2633.s3759621.Assignment1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Reducer class for all Tasks. It adds up the:
 * - Number of words for Tasks 1 and 4 (i.e. the short, medium, long and extralong words)
 *      -> Emit the (key, value) pair containing the type of word as key and the count as value
 * - Number of words for Task 2  (i.e. words that start with a vowel and those that start
 *          with a consonant)
 *      -> Emit the (key, value) pair containing the type of first character as key and
 *                 the count as value
 * - Number of words for Task 3 after in-mapping combiner
 *      -> Emit the (key, value) pain containing the word as key and the count as value
 *
 * Sources:
 * Lab 3, WordCount task, Task2code.txt
 */
public class GeneralReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    private static final Logger LOG = Logger.getLogger(GeneralReducer.class);
    private IntWritable result = new IntWritable();

    public void reduce (Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        LOG.setLevel(Level.DEBUG);
        LOG.debug("The reducer task of Oana-Madalina Ivanovici, s3759621");

        int count = 0;
        for (IntWritable val : values) {
            count += val.get();
        }
        result.set(count);
        context.write(key, result);

    }
}
