package com.latticeengines.dataflow.runtime.cascading.propdata.util.stats.bucket;

import java.util.List;

public abstract class BucketUtil {
    protected static final String LABEL_NUMERICAL_END_PLUS = "+";
    protected static final String LABEL_NUMERICAL_SEPARATOR = "-";

    abstract boolean accepts(Class<?> type);

    abstract String findBucket(List<Object> buckets, List<String> bucketLbls, //
            Object valObj);

    abstract List<Object> calculateBuckets(List<Object> minMaxList, //
            int maxBucketCount);

    abstract String getHigherPartOfLbl(Object lowerObject, Object higherObject);

    public String calculateLabel(List<Object> buckets, int i) {
        String lbl = (i + 1 == buckets.size()) ? buckets.get(i) + LABEL_NUMERICAL_END_PLUS
                : buckets.get(i) + (getHigherPartOfLbl(buckets.get(i), buckets.get(i + 1)));
        return lbl;
    }
}
