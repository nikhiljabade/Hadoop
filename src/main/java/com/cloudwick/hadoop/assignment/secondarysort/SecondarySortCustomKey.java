package com.cloudwick.hadoop.assignment.secondarysort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SecondarySortCustomKey implements Writable, WritableComparable<SecondarySortCustomKey>{
    private IntWritable reviewerID = new IntWritable();
    private IntWritable rating = new IntWritable();

    public IntWritable getReviewerID() {
        return reviewerID;
    }

    public void setReviewerID(Integer reviewerID) {
        this.reviewerID.set(reviewerID);
    }

    public IntWritable getRating() {
        return rating;
    }

    public void setRating(Integer rating) {
        this.rating.set(rating);
    }

    public void write(DataOutput dataOutput) throws IOException {
        reviewerID.write(dataOutput);
        rating.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        reviewerID.readFields(dataInput);
        rating.readFields(dataInput);
    }

    public static SecondarySortCustomKey read(DataInput in) throws Exception {
        SecondarySortCustomKey key = new SecondarySortCustomKey();
        key.readFields(in);
        return key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        SecondarySortCustomKey customKey = (SecondarySortCustomKey) o;

        return reviewerID.equals(customKey.reviewerID);
    }

    @Override
    public int hashCode() {
        return reviewerID.hashCode();
    }

    @Override
    public int compareTo(SecondarySortCustomKey o) {
        int comparedValue = reviewerID.compareTo(o.reviewerID);
        if (comparedValue == 0) {
             comparedValue = rating.compareTo(o.getRating());
        }

        return comparedValue;
    }

    public String toString() {
        return reviewerID + " : " + rating;
    }

}
