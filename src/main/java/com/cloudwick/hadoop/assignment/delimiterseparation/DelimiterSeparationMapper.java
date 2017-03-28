package com.cloudwick.hadoop.assignment.delimiterseparation;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class DelimiterSeparationMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        context.write(NullWritable.get(), value);
    }
}
