package com.latticeengines.dataflow.runtime.cascading.propdata.util.stats.bucket;

import java.util.ArrayList;
import java.util.List;

public class StatsLabelUtil {
    private static List<BucketUtil> bucketUtils;

    static {
        bucketUtils = new ArrayList<>();
        bucketUtils.add(new IntBucketUtil());
        bucketUtils.add(new LongBucketUtil());
        bucketUtils.add(new DoubleBucketUtil());
    }

    public List<String> getBucketLabels(List<Object> buckets) {
        List<String> bucketLbls = new ArrayList<>();
        for (int i = 0; i < buckets.size(); i++) {
            String lbl = getBucketUtil(buckets.get(0)).calculateLabel(buckets, i);
            bucketLbls.add(lbl);
        }
        return bucketLbls;
    }

    public String getMatchingBucketLbl(Object obj, List<Object> buckets, List<String> bucketLbls) {
        return getBucketUtil(obj).findBucket(buckets, bucketLbls, obj);
    }

    public List<Object> getBuckets(Object obj, List<Object> minMaxList, int maxBucketCount) {
        return getBucketUtil(obj).calculateBuckets(minMaxList, maxBucketCount);
    }

    private BucketUtil getBucketUtil(Object obj) {
        for (BucketUtil util : bucketUtils) {
            if (util.accepts(obj.getClass())) {
                return util;
            }
        }
        throw new RuntimeException("Could not find bucket util for class " + obj.getClass());
    }

}
