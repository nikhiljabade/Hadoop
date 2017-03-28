package com.cloudwick.hadoop.assignment.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondarySortGroupComparator extends WritableComparator {

    protected SecondarySortGroupComparator() {
        super(SecondarySortCustomKey.class, true);
    }

    public int compare(WritableComparable wc1, WritableComparable wc2) {
        SecondarySortCustomKey customKeyOne = (SecondarySortCustomKey) wc1;
        SecondarySortCustomKey customKeyTwo = (SecondarySortCustomKey) wc2;
        return customKeyOne.getReviewerID().compareTo(customKeyTwo.getReviewerID());
    }
}
