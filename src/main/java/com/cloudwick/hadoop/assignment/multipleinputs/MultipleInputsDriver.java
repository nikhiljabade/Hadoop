package com.cloudwick.hadoop.assignment.multipleinputs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MultipleInputsDriver extends Configured implements Tool{
    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new MultipleInputsDriver(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        Path inputPathOne = new Path("./src/main/java/com/cloudwick/hadoop/assignment/multipleinputs/MultipleInputsInput/multipleinputsairline.txt");
        Path inputPathTwo = new Path("./src/main/java/com/cloudwick/hadoop/assignment/multipleinputs/MultipleInputsInput/multipleinputsbooks.txt");
        Path outputPath = new Path("./src/main/java/com/cloudwick/hadoop/assignment/multipleinputs/MultipleInputsOutput");

        Configuration conf = getConf();
        Job job = new Job(conf, this.getClass().toString());
        FileSystem fileSystem = FileSystem.get(conf);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

        MultipleInputs.addInputPath(job, inputPathOne, TextInputFormat.class, MultipleInputsMapperOne.class);
        MultipleInputs.addInputPath(job, inputPathTwo, TextInputFormat.class, MultipleInputsMapperTwo.class);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setJobName("MultipleInputs");
        job.setJarByClass(MultipleInputsDriver.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}