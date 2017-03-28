package com.cloudwick.hadoop.assignment.secondarysort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondarySortSortComparator extends WritableComparator {

    protected SecondarySortSortComparator() {
        super(SecondarySortCustomKey.class, true);
    }

    public int compare(WritableComparable wc1, WritableComparable wc2) {
        SecondarySortCustomKey customKeyOne = (SecondarySortCustomKey) wc1;
        SecondarySortCustomKey customKeyTwo = (SecondarySortCustomKey) wc2;
        int cmp = customKeyOne.getReviewerID().compareTo(customKeyTwo.getReviewerID());
        if (cmp != 0) {
            return cmp;
        }
        return customKeyTwo.getRating().compareTo(customKeyOne.getRating()); //reverse
    }
}
