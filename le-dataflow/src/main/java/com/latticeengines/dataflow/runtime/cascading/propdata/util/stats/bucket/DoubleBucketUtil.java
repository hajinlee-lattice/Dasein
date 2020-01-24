package com.latticeengines.dataflow.runtime.cascading.propdata.util.stats.bucket;

import java.util.ArrayList;
import java.util.List;

public class DoubleBucketUtil extends BucketUtil {
    @Override
    public boolean accepts(Class<?> type) {
        return type.isAssignableFrom(Double.class);
    }

    @Override
    public String findBucket(List<Object> buckets, List<String> bucketLbls, Object valObj) {
        Double val = (Double) valObj;
        String lbl = "";
        for (int i = 0; i < buckets.size(); i++) {
            Double bucketA = (Double) buckets.get(i);
            if (val >= bucketA) {
                if (val.equals(bucketA)) {
                    lbl = bucketLbls.get(i);
                    break;
                } else {
                    if (i + 1 >= buckets.size()) {
                        lbl = bucketLbls.get(i);
                        break;
                    } else {
                        Double bucketB = (Double) buckets.get(i + 1);
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

    @Override
    public List<Object> calculateBuckets(List<Object> minMaxList, //
            int maxBucketCount) {
        List<Object> buckets = new ArrayList<>();
        Double min = (Double) minMaxList.get(0);
        Double max = (Double) minMaxList.get(1);

        Double diff = max - min;

        if (diff == 0) {
            buckets.add(min);
        } else if (diff <= maxBucketCount) {
            for (int i = 0; i < diff; i++) {
                buckets.add(min + i);
            }
        } else {
            Double width = diff / maxBucketCount;
            for (int i = 0; i < maxBucketCount; i++) {
                buckets.add(min + i * width);
            }
        }
        return buckets;
    }

    @Override
    public String getHigherPartOfLbl(Object lowerObject, Object higherObject) {
        Double obj = ((Double) higherObject);
        return LABEL_NUMERICAL_SEPARATOR + obj;
    }
}
