package org.mbds;

import java.io.IOException;

import java.util.Arrays;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class SalesAnalysis {

    public static class TotalMap extends Mapper<LongWritable, Text, Text, FloatWritable> {
        private Text sortedWord; // Text object which Will be used as the key
        private FloatWritable total;



        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            int choix = conf.getInt("org.mbds.SalesAnalysis",0);

            String line = value.toString();
            line.lines().skip(1);

            String[] columns = line.split(",");

            if (choix ==0)   sortedWord.set(columns[0]);
            else sortedWord.set(columns[1]);

            total = new FloatWritable(Float.parseFloat(columns[2])); // Price
            context.write(sortedWord, total); // This context object represents the Key-Value pair output of the mapper (Context object: Allows the Mapper/Reducer to interact with the rest of the Hadoop system)

        }
    }

    public static class TotalReduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        private FloatWritable result = new FloatWritable();

        @Override
        protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (FloatWritable value : values) {
                sum += value.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // Instantiate the Hadoop Configuration.
        Configuration conf = new Configuration();

        // Parse command-line arguments.
        // The GenericOptionParser takes care of Hadoop-specific arguments.
        String[] ourArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // Check input arguments.
        if (ourArgs.length != 3) {
            System.err.println("Usage: Total sales <arg> <in> <out>");
            System.exit(2);
        }

        // Get a Job instance.
        Job job = Job.getInstance(conf, "Total Sales");
        int choix = 0;
        if (ourArgs[0].equals("--country")) choix=1;

        conf.setInt("org.mbds.SalesAnalysis",choix);


        // Setup the Driver/Mapper/Reducer classes.
        job.setJarByClass(SalesAnalysis.class);
        job.setMapperClass(SalesAnalysis.TotalMap.class);
        job.setReducerClass(SalesAnalysis.TotalReduce.class);
        // Indicate the key/value output types we are using in our Mapper & Reducer.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        // Indicate from where to read input data from HDFS.
        FileInputFormat.addInputPath(job, new Path(ourArgs[1]));
        // Indicate where to write the results on HDFS.
        FileOutputFormat.setOutputPath(job, new Path(ourArgs[2]));

        // We start the MapReduce Job execution (synchronous approach).
        // If it completes with success we exit with code 0, else with code 1.
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }




}
