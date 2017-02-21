package com.latticeengines.dataflow.runtime.cascading.propdata.util.stats.handlers;

import java.util.HashMap;
import java.util.Map;

public class BooleanHandler {
    public void handleBooleanAttribute(Map<String, Map<String, Long>> attributeValueBuckets, String fieldName,
            Boolean objVal, Map<String, Map<String, Long>> nAttributeBucketIds) {
        if (!attributeValueBuckets.containsKey(fieldName)) {
            attributeValueBuckets.put(fieldName, new HashMap<String, Long>());
            nAttributeBucketIds.put(fieldName, new HashMap<String, Long>());

            Map<String, Long> fieldBucketIds = nAttributeBucketIds.get(fieldName);
            if (objVal != null) {
                if (objVal.booleanValue() == true) {
                    fieldBucketIds.put(objVal.toString(), 0L);
                } else {
                    fieldBucketIds.put(objVal.toString(), 1L);
                }
            }

        }

        Map<String, Long> fieldBucketMap = attributeValueBuckets.get(fieldName);
        if (!fieldBucketMap.containsKey(objVal.toString())) {
            fieldBucketMap.put(objVal.toString(), 0L);
        }

        Long bucketCount = fieldBucketMap.get(objVal.toString());
        fieldBucketMap.put(objVal.toString(), ++bucketCount);
    }

}
