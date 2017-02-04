package com.latticeengines.dataflow.runtime.cascading.propdata.util.stats.handlers;

import java.util.HashMap;
import java.util.Map;

public class BooleanTextHandler {
    public void handleBooleanTextAttribute(Map<String, Map<String, Long>> attributeValueBuckets, String fieldName,
            String objVal) {
        if (!attributeValueBuckets.containsKey(fieldName)) {
            attributeValueBuckets.put(fieldName, new HashMap<String, Long>());
        }

        Map<String, Long> fieldBucketMap = attributeValueBuckets.get(fieldName);
        if (!fieldBucketMap.containsKey(objVal)) {
            fieldBucketMap.put(objVal, 0L);
        }

        Long bucketCount = fieldBucketMap.get(objVal);
        fieldBucketMap.put(objVal, ++bucketCount);
    }
}
