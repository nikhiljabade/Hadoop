package com.cloudwick.hadoop.assignment.ip2geo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Ip2GeoDriver extends Configured implements Tool {
    public static void main(String args[]) throws Exception {
        int res = ToolRunner.run(new Ip2GeoDriver(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Path inputPathOne = new Path("./src/main/java/com/cloudwick/hadoop/assignment/ip2geo/Ip2GeoInput/ip2geooneinput.txt");
        Path inputPathTwo = new Path("./src/main/java/com/cloudwick/hadoop/assignment/ip2geo/Ip2GeoInput/ip2geotwoinput.txt");
        Path outputPath = new Path("./src/main/java/com/cloudwick/hadoop/assignment/ip2geo/Ip2GeoOutput");

        Configuration conf = getConf();
        Job job = new Job(conf, this.getClass().toString());
        FileSystem fileSystem = FileSystem.get(conf);
        if(fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }

        MultipleInputs.addInputPath(job, inputPathOne, TextInputFormat.class, Ip2GeoMapperOne.class);
        MultipleInputs.addInputPath(job, inputPathTwo, TextInputFormat.class, Ip2GeoMapperTwo.class);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setJobName("Ip2GeoData");
        job.setJarByClass(Ip2GeoDriver.class);

        job.setReducerClass(Ip2GeoReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}