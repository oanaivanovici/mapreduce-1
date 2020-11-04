package edu.rmit.cosc2633.s3759621.Assignment1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Partitioner class for Task 4. Short and extra long words are directed to partition 0,
 * medium and long words to partition 1, and other words that don't fit these four criteria
 * are directed to partition 2.
 * Sources:
 * Lab 4, highestSalary task, HighestSalaryPartitioner.java class
 */

public class WordLengthExtendedPartitioner extends Partitioner<Text, IntWritable> {

    private static final Logger LOG = Logger.getLogger(WordLengthExtendedPartitioner.class);

    @Override
    public int getPartition(Text key, IntWritable value, int numPartitions) {

        LOG.setLevel(Level.DEBUG);
        LOG.debug("The partitioner task of Oana-Madalina Ivanovici, s3759621");

        String wordType = key.toString();
        LOG.debug("The word type is: " + wordType);

        /* If no partitions */
        if (numPartitions == 0) {
            LOG.debug("There is no partition");
            return 0;
        }

        /* If word is short or extra long, send to partition 0 */
        if (wordType.equals(WordLengthMapper.shortWord.toString()) ||
                wordType.equals(WordLengthMapper.extralongWord.toString())) {
            LOG.debug("Words of type " + WordLengthMapper.shortWord + " and " +
                    WordLengthMapper.extralongWord + " go to partition 0");
            return 0;
        }
        /* If word is medium or long, send to partition 1 */
        else if (wordType.equals(WordLengthMapper.mediumWord.toString()) ||
                wordType.equals(WordLengthMapper.longWord.toString())) {
            LOG.debug("Words of type " + WordLengthMapper.mediumWord + " and " +
                    WordLengthMapper.longWord + " go to partition 1");
            return 1 % numPartitions;
        }
        /* For all other words send to partition 2 */
        else {
            LOG.debug("Words that are in none of the four categories go to partition 2");
            return 2 % numPartitions;
        }

    }
}
