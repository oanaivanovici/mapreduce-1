package edu.rmit.cosc2633.s3759621.Assignment1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Mapper class for Task 1 work out the length of each word.
 * Sources:
 * Lab 3, WordCount task, Task2code.txt
 * Lab 4, highestSalary task, HighestSalaryMapper.java class
 */

public class WordLengthMapper extends Mapper<Object, Text, Text, IntWritable> {

	private static final Logger LOG = Logger.getLogger(WordLengthMapper.class);

	private final static int ONE = 1;
	private final static int FOUR = 4;
	private final static int FIVE = 5;
	private final static int SEVEN = 7;
	private final static int EIGHT = 8;
	private final static int TEN = 10;

	protected final static Text shortWord = new Text("short word");
    protected final static Text mediumWord = new Text("medium word");
    protected final static Text longWord = new Text("long word");
    protected final static Text extralongWord = new Text("extra-long word");

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        LOG.setLevel(Level.DEBUG);
        LOG.debug("The mapper task of Oana-Madalina Ivanovici, s3759621");

        /* Tokenise, format the word (keep only letters a-z, digits 0-9, dashes and
        apostrophes), iterate through words/tokens */
	    StringTokenizer itr = new StringTokenizer(value.toString().replaceAll("[^\\p{Alnum}\\s\\'\\-]", " "));
		while (itr.hasMoreTokens()) {
			word.set(itr.nextToken());
			String formattedWord = word.toString();

			/* If the length of the formatted word is between 1 and 4 letters -> short word
			* Emit one count of 'shortWord' */
			if (formattedWord.length() >= ONE && formattedWord.length() <= FOUR) {
			    LOG.debug(formattedWord + " is of length " + formattedWord.length() + " therefore we consider it a " + shortWord);
			    context.write(shortWord, one);
            }
            /* If the length of the formatted word is between 5 and 7 letters -> medium word
            * Emit one count of 'mediumWord' */
            else if (formattedWord.length() >= FIVE && formattedWord.length() <= SEVEN) {
                LOG.debug(formattedWord + " is of length " + formattedWord.length() + " therefore we consider it a " + mediumWord);
                context.write(mediumWord, one);
            }
            /* If the length of the formatted word is between 8 and 10 letters -> long word
            * Emit one count of 'longWord' */
            else if (formattedWord.length() >= EIGHT && formattedWord.length() <= TEN) {
                LOG.debug(formattedWord + " is of length " + formattedWord.length() + " therefore we consider it a " + longWord);
                context.write(longWord, one);
            }
            /* Otherwise if the length of the formatted word is more than 10 letters -> extralong word
            * Emit one count of 'extralongWord' */
            else if (formattedWord.length() > TEN) {
                LOG.debug(formattedWord + " is of length " + formattedWord.length() + " therefore we consider it a " + extralongWord);
                context.write(extralongWord, one);
            }
		}
	}
}
