package com.cloudwick.hadoop.assignment.textpaircount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class TextPairMapper extends Mapper<LongWritable, Text, TextPairCustomKey, IntWritable> {
    IntWritable one = new IntWritable(1);

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = value.toString().split(":");
        TextPairCustomKey textPairCustomKey = new TextPairCustomKey();
        textPairCustomKey.setState(new Text(columns[0]));
        textPairCustomKey.setCity(new Text(columns[1]));
        context.write(textPairCustomKey, one);
    }

}
