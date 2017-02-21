package com.latticeengines.dataflow.runtime.cascading.propdata.util.stats.handlers;

import java.util.HashMap;
import java.util.Map;

public class BooleanTextHandler {
    public void handleBooleanTextAttribute(Map<String, Map<String, Long>> attributeValueBuckets, String fieldName,
            String objVal, Map<String, Map<String, Long>> nAttributeBucketIds) {
        if (!attributeValueBuckets.containsKey(fieldName)) {
            attributeValueBuckets.put(fieldName, new HashMap<String, Long>());
            nAttributeBucketIds.put(fieldName, new HashMap<String, Long>());

            Map<String, Long> fieldBucketIds = nAttributeBucketIds.get(fieldName);
            if (objVal != null) {
                Long id = 0L;
                id = assignIdForBooleanText(objVal);
                fieldBucketIds.put(objVal, id);
            }
        }

        Map<String, Long> fieldBucketMap = attributeValueBuckets.get(fieldName);

        if (!fieldBucketMap.containsKey(objVal)) {
            fieldBucketMap.put(objVal, 0L);
        }

        Long bucketCount = fieldBucketMap.get(objVal);
        fieldBucketMap.put(objVal, ++bucketCount);
    }

    public Long assignIdForBooleanText(String objVal) {
        Long id;
        if (objVal.toUpperCase().startsWith("Y")//
                || objVal.toUpperCase().startsWith("T")//
                || objVal.toUpperCase().startsWith("1")) {
            id = 0L;
        } else {
            id = 1L;
        }
        return id;
    }
}
