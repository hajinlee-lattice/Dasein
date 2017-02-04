package com.latticeengines.dataflow.runtime.cascading.propdata.util.stats.handlers;

import java.util.HashMap;
import java.util.Map;

public class BooleanHandler {
    public void handleBooleanAttribute(Map<String, Map<String, Long>> attributeValueBuckets, String fieldName,
            Boolean objVal) {
        if (!attributeValueBuckets.containsKey(fieldName)) {
            attributeValueBuckets.put(fieldName, new HashMap<String, Long>());
        }

        Map<String, Long> fieldBucketMap = attributeValueBuckets.get(fieldName);
        if (!fieldBucketMap.containsKey(objVal.toString())) {
            fieldBucketMap.put(objVal.toString(), 0L);
        }

        Long bucketCount = fieldBucketMap.get(objVal.toString());
        fieldBucketMap.put(objVal.toString(), ++bucketCount);
    }

}
