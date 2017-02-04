package com.latticeengines.dataflow.runtime.cascading.propdata.util.stats.handlers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.dataflow.runtime.cascading.propdata.util.stats.bucket.StatsLabelUtil;

public class NumericHandler {
    private static StatsLabelUtil labelUtl = new StatsLabelUtil();

    public void handleNumericalAttribute(Map<String, Map<String, Long>> attributeValueBucket, Object obj,
            String fieldName, Map<String, List<String>> bucketLblOrderMap, List<Object> minMaxList,
            Map<String, List<Object>> bucketOrderMap, int maxBucketCount) {
        Object objVal = obj;
        if (!attributeValueBucket.containsKey(fieldName)) {
            List<Object> buckets = labelUtl.getBuckets(obj, minMaxList, maxBucketCount);
            List<String> bucketLbls = labelUtl.getBucketLabels(buckets);

            attributeValueBucket.put(fieldName, new HashMap<String, Long>());
            bucketOrderMap.put(fieldName, buckets);
            bucketLblOrderMap.put(fieldName, bucketLbls);
        }

        List<Object> buckets = bucketOrderMap.get(fieldName);
        List<String> bucketLbls = bucketLblOrderMap.get(fieldName);

        Map<String, Long> fieldBucketMap = attributeValueBucket.get(fieldName);
        String bucketLbl = labelUtl.getMatchingBucketLbl(objVal, buckets, bucketLbls);

        if (!fieldBucketMap.containsKey(bucketLbl)) {
            fieldBucketMap.put(bucketLbl, 0L);
        }

        Long count = fieldBucketMap.get(bucketLbl);
        fieldBucketMap.put(bucketLbl, ++count);
    }

}
