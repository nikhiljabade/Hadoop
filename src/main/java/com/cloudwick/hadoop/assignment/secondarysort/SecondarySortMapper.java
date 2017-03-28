package com.cloudwick.hadoop.assignment.secondarysort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class SecondarySortMapper extends Mapper<LongWritable, Text, SecondarySortCustomKey, NullWritable> {
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] columns = value.toString().split(" ");
        SecondarySortCustomKey customKey = new SecondarySortCustomKey();
        customKey.setReviewerID(Integer.parseInt(columns[0]));
        customKey.setRating(Integer.parseInt(columns[2]));
        context.write(customKey, NullWritable.get());
    }
}
