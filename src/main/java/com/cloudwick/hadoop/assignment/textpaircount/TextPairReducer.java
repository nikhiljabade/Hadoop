package com.cloudwick.hadoop.assignment.textpaircount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class TextPairReducer extends Reducer<TextPairCustomKey, IntWritable, TextPairCustomKey, IntWritable> {
    public void reduce(TextPairCustomKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        for(IntWritable value : values) {
            sum += value.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
