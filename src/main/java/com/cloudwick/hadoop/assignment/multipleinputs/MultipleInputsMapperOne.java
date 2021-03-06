package com.cloudwick.hadoop.assignment.multipleinputs;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MultipleInputsMapperOne extends Mapper<LongWritable, Text, NullWritable, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        context.write(NullWritable.get(), value);
    }
}
