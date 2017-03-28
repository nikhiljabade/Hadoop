package com.cloudwick.hadoop.assignment.aggregations;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class AggregationReducer extends Reducer<NullWritable, DoubleWritable, Text, DoubleWritable>{

    double average = 0.0, min = 0.0, max = 0.0, sum = 0.0;
    int count = 0;

    public void reduce(NullWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        for (DoubleWritable value : values) {
        //    System.out.println("Values -----> " + value.get());
            count++;
            sum = sum + value.get();
            if(count == 1) {
                min = value.get();
                max = value.get();
            }
            else {
                if(min > value.get()) {
                    min = value.get();
                }
                if(max < value.get()) {
                    max = value.get();
                }
            }
        }

        average = sum / count;

        DoubleWritable valueData = new DoubleWritable();
        Text keyData = new Text();
        keyData.set("Average: ");
        valueData.set(average);
        context.write(keyData, valueData);
        keyData.set("Count: ");
        valueData.set(count);
        context.write(keyData, valueData);
        keyData.set("Sum: ");
        valueData.set(sum);
        context.write(keyData, valueData);
        keyData.set("Max: ");
        valueData.set(max);
        context.write(keyData, valueData);
        keyData.set("MIn: ");
        valueData.set(min);
        context.write(keyData, valueData);
    }
}
