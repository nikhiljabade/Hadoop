package com.cloudwick.hadoop.assignment.secondarysort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SecondarySortDriver extends Configured implements Tool {
    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new SecondarySortDriver(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Path inputPath = new Path("./src/main/java/com/cloudwick/hadoop/assignment/secondarysort/SecondarySortInput/secondarysortinput.txt");
        Path outputPath = new Path("./src/main/java/com/cloudwick/hadoop/assignment/secondarysort/SecondarySortOutput");

        Configuration conf = getConf();
        Job job = new Job(conf, this.getClass().toString());
        FileSystem fileSystem = FileSystem.get(conf);
        if(fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setJobName("SecondarySortData");
        job.setJarByClass(SecondarySortDriver.class);

        job.setMapperClass(SecondarySortMapper.class);
        job.setReducerClass(SecondarySortReducer.class);
        job.setPartitionerClass(SecondarySortPartitioner.class);
        job.setGroupingComparatorClass(SecondarySortGroupComparator.class);
        job.setSortComparatorClass(SecondarySortSortComparator.class);

        job.setMapOutputKeyClass(SecondarySortCustomKey.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(SecondarySortCustomKey.class);
        job.setOutputValueClass(NullWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
