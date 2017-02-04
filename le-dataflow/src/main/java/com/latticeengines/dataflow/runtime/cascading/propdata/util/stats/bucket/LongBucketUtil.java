package com.latticeengines.dataflow.runtime.cascading.propdata.util.stats.bucket;

import java.util.ArrayList;
import java.util.List;

public class LongBucketUtil extends BucketUtil {
    @Override
    public boolean accepts(Class<?> type) {
        return type.isAssignableFrom(Long.class);
    }

    @Override
    public String findBucket(List<Object> buckets, List<String> bucketLbls, Object valObj) {
        Long val = (Long) valObj;
        String lbl = "";
        for (int i = 0; i < buckets.size(); i++) {
            Long bucketA = (Long) buckets.get(i);

            if (val.longValue() < bucketA.longValue()) {

            } else if (val.equals(bucketA)) {
                lbl = bucketLbls.get(i);
                break;
            } else if (val.longValue() > bucketA.longValue()) {
                if (i + 1 >= buckets.size()) {
                    lbl = bucketLbls.get(i);
                    break;
                } else {
                    Long bucketB = (Long) buckets.get(i + 1);
                    if (val.longValue() < bucketB.longValue()) {
                        lbl = bucketLbls.get(i);
                        break;
                    }
                }
            }
        }
        return lbl;
    }

    @Override
    public List<Object> calculateBuckets(List<Object> minMaxList, //
            int maxBucketCount) {
        List<Object> buckets = new ArrayList<>();
        Long min = 0L;
        Long max = 0L;

        if (minMaxList.get(0) instanceof Integer) {
            min = ((Integer) minMaxList.get(0)).longValue();
        } else {
            min = (Long) minMaxList.get(0);
        }
        if (minMaxList.get(1) instanceof Integer) {
            max = ((Integer) minMaxList.get(1)).longValue();
        } else {
            max = (Long) minMaxList.get(1);
        }

        Long diff = max - min;

        if (diff == 0) {
            buckets.add(min);
        } else if (diff <= maxBucketCount) {
            for (int i = 0; i < diff; i++) {
                buckets.add(min + i);
            }
        } else {
            Long width = diff / maxBucketCount;
            for (int i = 0; i < maxBucketCount; i++) {
                buckets.add(min + i * width);
            }
        }
        return buckets;
    }

    @Override
    public String getHigherPartOfLbl(Object lowerObject, Object higherObject) {
        Long obj = ((Long) higherObject);
        return LABEL_NUMERICAL_SEPARATOR + obj;
    }
}
