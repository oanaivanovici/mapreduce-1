MapReduce project for course "Big Data Processing" at RMIT University, Melbourne, Australia.

Contains four tasks:
1. Count number of short words (1-4 letters), medium words (5-7 letters) words, long words (8-10 letters) and extra-long words (More than 10 letters). This task can be run using the standalone jar in the target folder and the following command:
hadoop jar Assignment1-1.0-SNAPSHOT.jar edu.rmit.cosc2633.s3759621.Assignment1.WordLength <input_path> <output_path>
2. Count of all words that begin with a vowel and count of all how many words that begin with a consonant. This task can be run using the standalone jar in the target folder and the following command:
hadoop jar Assignment1-1.0-SNAPSHOT.jar edu.rmit.cosc2633.s3759621.Assignment1.WordFirstChar <input_path> <output_path>
3. Count the number of each word where the in-mapper combining is implemented rather than an independent combiner. This task can be run using the standalone jar in the target folder and the following command:
hadoop jar Assignment1-1.0-SNAPSHOT.jar edu.rmit.cosc2633.s3759621.Assignment1.WordCount <input_path> <output_path>
4. Count word with partitioner, extending Task 1 by using partitioner such that
- short words (1-4 letters) and extra-long words (More than 10 letters) are processed in one reducer,
- medium words (5-7 letters) and long words (8-10 letters) are processed in another reducer.
This task can be run using the standalone jar in the target folder and the following command:
hadoop jar Assignment1-1.0-SNAPSHOT.jar edu.rmit.cosc2633.s3759621.Assignment1.WordLengthExtended <input_path> <output_path>
