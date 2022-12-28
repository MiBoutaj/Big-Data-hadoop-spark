package org.mbds;

import java.io.IOException;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Anagrams {
      public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private Text sortedWord; // Text object which Will be used as the key
        private Text word;// Text object which will be used as the value

        // Method that sorts a string alphabetically
        private static String sortStringAlphabetically(String inputString)
        {
            // Converting input string to character array
            char tempCharArray[] = inputString.toCharArray();

            // Sorting temp array using sort (by default alphabetically sorting)
            Arrays.sort(tempCharArray);

            // Returning a new string with the sorted characters of the original inputString
            return new String(tempCharArray);
        }

        // Overwritten method of Mapper
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Note: For this case of map-phase, the input key is "ignored" since it is just the line-offset, and it is not being used
            String line = value.toString(); // Transform the value (input line) from Text object to String
            word = new Text(line); // The Text object which Will be used as the value
            sortedWord = new Text(sortStringAlphabetically(line));// The Text object which will be used as the key (sorted input alphabetically)

            // System.out.println("Word read (Value): " + word.toString() + ", Sorted Word (Key): " + sortedWord.toString());
            context.write(sortedWord, word); // This context object represents the Key-Value pair output of the mapper (Context object: Allows the Mapper/Reducer to interact with the rest of the Hadoop system)

        }
    }

     public static class Reduce extends Reducer<Text, Text, LongWritable, Text> {

        // Overwritten method of Reducer
        public void reduce(Text key, Iterable<Text> values, Context context) // values should contain the read words with the same key
                throws IOException, InterruptedException {
            // Iterator to traverse the Iterable
            Iterator<Text> iterator = values.iterator();

            long total_anagrams = 1; // Initialize counter for total number of words with the same key
            String anagrams = iterator.next().toString(); // Initialize a string which will contain all the anagrams separated with commas

            while (iterator.hasNext()){
                anagrams = anagrams.concat("," + iterator.next().toString());
                total_anagrams++;
            }

            // It is only desirable to print anagrams (2 or more words with same key), thus we do not write the case of just single words without any anagrams
            if (total_anagrams > 1)
                // Using the count as the output "key", since we don't really have a specific key that needs to be printed as the output
                context.write(new LongWritable(total_anagrams), new Text(anagrams)); // This context object represents the Key-Value pair output of the reducer and it is going to be given as the final output (Context object: Allows the Mapper/Reducer to interact with the rest of the Hadoop system)
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] ourArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        Job job = Job.getInstance(conf, "anagram"); // The job is used like a wrapper, the name of the job is just for seeing which program-job is running
        job.setJarByClass(Anagrams.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class); // Mapper output value class, also input value class of reducer

        job.setMapperClass(Map.class);
        //job.setCombinerClass(Reduce.class); // In this case-task, the Reducer for Combiner is not reused, in order to not mess up the results
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(ourArgs[0]));
        // Indicate where to write the results on HDFS.
        FileOutputFormat.setOutputPath(job, new Path(ourArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}