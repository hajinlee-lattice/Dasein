package com.latticeengines.dataflow.runtime.cascading.propdata.util.stats.bucket;

import java.util.ArrayList;
import java.util.List;

public class IntBucketUtil extends BucketUtil {
    @Override
    public boolean accepts(Class<?> type) {
        return type.isAssignableFrom(Integer.class);
    }

    @Override
    public String findBucket(List<Object> buckets, List<String> bucketLbls, Object valObj) {
        Integer val = (Integer) valObj;
        String lbl = "";
        for (int i = 0; i < buckets.size(); i++) {
            Integer bucketA = (Integer) buckets.get(i);

            if (val >= bucketA) {
                if (val.equals(bucketA)) {
                    lbl = bucketLbls.get(i);
                    break;
                } else {
                    if (i + 1 >= buckets.size()) {
                        lbl = bucketLbls.get(i);
                        break;
                    } else {
                        Integer bucketB = (Integer) buckets.get(i + 1);
                        if (val < bucketB) {
                            lbl = bucketLbls.get(i);
                            break;
                        }
                    }
                }
            }
        }
        return lbl;
    }

    public List<Object> calculateBuckets(List<Object> minMaxList, //
            int maxBucketCount) {
        List<Object> buckets = new ArrayList<>();
        Integer min;
        Integer max;

        if (minMaxList.get(0) instanceof Integer) {
            min = (Integer) minMaxList.get(0);
        } else {
            min = ((Long) minMaxList.get(0)).intValue();
        }
        if (minMaxList.get(1) instanceof Integer) {
            max = (Integer) minMaxList.get(1);
        } else {
            max = ((Long) minMaxList.get(0)).intValue();
        }

        Integer diff = max - min;

        if (diff == 0) {
            buckets.add(min);
        } else if (diff <= maxBucketCount) {
            for (int i = 0; i < diff; i++) {
                buckets.add(min + i);
            }
        } else {
            Integer width = diff / maxBucketCount;
            for (int i = 0; i < maxBucketCount; i++) {
                buckets.add(min + i * width);
            }
        }
        return buckets;
    }

    @Override
    public String getHigherPartOfLbl(Object lowerObject, Object higherObject) {
        int objH = ((Integer) higherObject);
        int objL = ((Integer) lowerObject);
        return (objH - objL == 1) ? "" : LABEL_NUMERICAL_SEPARATOR + (objH - 1);
    }
}
