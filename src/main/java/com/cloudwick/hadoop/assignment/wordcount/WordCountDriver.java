package com.cloudwick.hadoop.assignment.wordcount;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;


public class WordCountDriver extends Configured implements Tool {
    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new WordCountDriver(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        Path inputPath = new Path("./src/main/java/com/cloudwick/hadoop/assignment/wordcount/WordCountInput/wordcountinput.txt");
        Path outputPath = new Path("./src/main/java/com/cloudwick/hadoop/assignment/wordcount/WordCountOutput");

        Configuration conf = getConf();
        @SuppressWarnings("deprecation")
        Job job = new Job(conf, this.getClass().toString());
        FileSystem fileSystem = FileSystem.get(conf);
        if(fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setJobName("WordCount");
        job.setJarByClass(WordCountDriver.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setReducerClass(WordCountReducer.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
