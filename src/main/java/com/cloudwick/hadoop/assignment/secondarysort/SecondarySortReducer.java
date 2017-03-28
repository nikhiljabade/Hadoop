package com.cloudwick.hadoop.assignment.secondarysort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class SecondarySortReducer extends Reducer<SecondarySortCustomKey, NullWritable, SecondarySortCustomKey, NullWritable> {
    public void reduce(SecondarySortCustomKey key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        for(NullWritable n : values) {
            context.write(key, NullWritable.get());
        }
    }
}
