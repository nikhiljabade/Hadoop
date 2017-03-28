package com.cloudwick.hadoop.assignment.secondarysort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class SecondarySortPartitioner extends Partitioner<SecondarySortCustomKey, NullWritable> {
    public int getPartition(SecondarySortCustomKey key, NullWritable value, int numberOfPartitions) {
        return Math.abs(key.getReviewerID().hashCode()) % numberOfPartitions;
    }
}
