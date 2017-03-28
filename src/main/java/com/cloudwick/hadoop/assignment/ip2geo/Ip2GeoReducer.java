package com.cloudwick.hadoop.assignment.ip2geo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class Ip2GeoReducer extends Reducer<Text, Text, Text, Text>{
    private String merge = "";
    private Text finalValue = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int i = 0;
        for(Text value : values) {
            if(i == 0)
                merge = value.toString() + " ";
            else
                merge = merge + value.toString();

            i++;
        }

        finalValue.set(merge);
        context.write(key, finalValue);
    }
}
