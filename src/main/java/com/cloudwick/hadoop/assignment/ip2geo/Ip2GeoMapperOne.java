package com.cloudwick.hadoop.assignment.ip2geo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class Ip2GeoMapperOne extends Mapper<LongWritable, Text, Text, Text> {
    private Text setKey = new Text();
    private Text setValue = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columns = line.split(" ");
        setKey.set(columns[2]);
        setValue.set(columns[0] + " " + columns[1]);
        context.write(setKey, setValue);
    }
}
