package com.cloudwick.hadoop.assignment.aggregations;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;

public class AggregationMapper extends Mapper<LongWritable, Text, NullWritable, DoubleWritable>{
    DoubleWritable doubletData = new DoubleWritable();

    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()) {
            doubletData.set(Double.parseDouble(tokenizer.nextToken()));
            context.write(NullWritable.get(), doubletData);
        }

    //    doubletData.set(Double.parseDouble(value.toString()));
    //    context.write(NullWritable.get(), doubletData);
    }
}
