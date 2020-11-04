package edu.rmit.cosc2633.s3759621.Assignment1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * Mapper class for Task 3.
 * Sources:
 * Lab 3, WordCount task, Task2code.txt
 * Lab 4, highestSalary task, HighestSalaryMapper.java class
 * [1] "Hadoop In-Mapper Combiner", Github.io, 2018. [Online]. Available: https://jinwooooo.github.io/jinwooooo-blog/hadoop-in-mapper-combiner. [Accessed: 07- Sep- 2020].
 */
public class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

    private static final Logger LOG = Logger.getLogger(WordCountMapper.class);
    public static HashMap<String, Integer> wordCounts;

    /**
     * Initialise hashmap of word counts
     * @param context the context
     */
    public void setup(Context context) {
        wordCounts = new HashMap<String, Integer>();
    }

    /**
     * The in-mapper combining method which uses the 'wordCounts' hashmap
     * @param key the key (word)
     * @param value the value (count of word)
     * @param context the context
     * @throws IOException
     * @throws InterruptedException
     */
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        LOG.setLevel(Level.DEBUG);
        LOG.debug("The mapper task of Oana-Madalina Ivanovici, s3759621");

        /* Tokenise, remove all commas and dots, iterate through words/tokens */
        StringTokenizer itr = new StringTokenizer(value.toString().replaceAll("[^\\p{Alnum}\\s\\'\\-]", " "));
        while (itr.hasMoreTokens()) {
            String wordToken = itr.nextToken().toLowerCase();

            /* If the word is already in the hashmap, increase the count. Otherwise, set the count to 1 */
            if (wordCounts.containsKey(wordToken)) {
                int newCount = wordCounts.get(wordToken) + 1;
                wordCounts.put(wordToken, newCount);
            } else {
                wordCounts.put(wordToken, 1);
            }
        }
    }

    /**
     * Cleanup method which iterates over keys in 'wordCounts' hashmap and emits the (key, value)
     * pairs of words and their respective count
     * @param context the context
     * @throws IOException
     * @throws InterruptedException
     */
    public void cleanup(Context context) throws IOException, InterruptedException {

        for (Map.Entry<String, Integer> countsEntry: wordCounts.entrySet()) {
            LOG.debug("Word: " + countsEntry.getKey() + " Count: " + countsEntry.getValue());
            Text word = new Text(countsEntry.getKey());
            IntWritable count = new IntWritable(countsEntry.getValue());
            context.write(word, count);
        }

    }
}
