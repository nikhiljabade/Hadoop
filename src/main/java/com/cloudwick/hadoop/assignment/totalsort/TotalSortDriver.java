package com.cloudwick.hadoop.assignment.totalsort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TotalSortDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new TotalSortDriver(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Path inputPath = new Path("./src/main/java/com/cloudwick/hadoop/assignment/totalsort/TotalSortInput/totalsortinput.txt");
        Path outputPath = new Path("./src/main/java/com/cloudwick/hadoop/assignment/totalsort/TotalSortOutput");

        Configuration conf = getConf();
        Job job = new Job(conf, this.getClass().toString());
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setJobName("TotalSortData");
        job.setJarByClass(TotalSortDriver.class);
        job.setNumReduceTasks(5);
        job.setInputFormatClass(TextInputFormat.class);
        job.setPartitionerClass(TotalOrderPartitioner.class);
        InputSampler.Sampler<LongWritable, NullWritable> sampler = new InputSampler.RandomSampler<LongWritable, NullWritable>(0.5, 5, 1);
        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), new Path("intermediate"));
        job.setMapperClass(TotalSortMapper.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(NullWritable.class);
        InputSampler.writePartitionFile(job, sampler);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
