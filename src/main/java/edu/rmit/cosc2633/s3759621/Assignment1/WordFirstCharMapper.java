package edu.rmit.cosc2633.s3759621.Assignment1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Mapper class for Task 2.
 * Sources:
 * Lab 3, WordCount task, Task2code.txt
 * Lab 4, highestSalary task, HighestSalaryMapper.java class
 */

public class WordFirstCharMapper extends Mapper<Object, Text, Text, IntWritable> {

    private static final Logger LOG = Logger.getLogger(WordFirstCharMapper.class);

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    private final static Text vowel = new Text("vowel");
    private final static Text consonant = new Text("consonant");
    private final static String stringConsonants = "BCDFGHJKLMNPQRSTVWXYZ";
    private final static String stringVowels = "AEIOU";

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        LOG.setLevel(Level.DEBUG);
        LOG.debug("The mapper task of Oana-Madalina Ivanovici, s3759621");

        /* Iterate through words/tokens */
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());

            /* Extract the first letter (in uppercase) from each word */
            Character firstLetter = Character.toUpperCase(word.toString().charAt(0));

            /* If the string of vowels contains the first letter -> word starts with vowel ->
             * Emit one count of 'vowel' word */
            if (stringVowels.indexOf(firstLetter) != -1) {
                LOG.debug(word + " begins with a vowel");
                context.write(vowel, one);
            } /* If the string of consonants contains the first letter -> word starts with a consonant
            -> Emit one count of 'consonant' word */
            else if (stringConsonants.indexOf(firstLetter) != -1) {
                LOG.debug(word + " begins with a consonant");
                context.write(consonant, one);
            }
        }

    }
}
