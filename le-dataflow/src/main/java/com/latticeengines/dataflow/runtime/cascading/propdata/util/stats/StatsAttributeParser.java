package com.latticeengines.dataflow.runtime.cascading.propdata.util.stats;

import java.util.List;
import java.util.Map;

import com.latticeengines.dataflow.runtime.cascading.propdata.util.stats.handlers.BooleanHandler;
import com.latticeengines.dataflow.runtime.cascading.propdata.util.stats.handlers.BooleanTextHandler;
import com.latticeengines.dataflow.runtime.cascading.propdata.util.stats.handlers.EncodedAttributeHandler;
import com.latticeengines.dataflow.runtime.cascading.propdata.util.stats.handlers.NumericHandler;
import com.latticeengines.domain.exposed.metadata.FundamentalType;

public class StatsAttributeParser {
    private BooleanHandler booleanHandler = new BooleanHandler();
    private BooleanTextHandler booleanTextHandler = new BooleanTextHandler();
    private EncodedAttributeHandler encodedAttributeHandler = new EncodedAttributeHandler();
    private NumericHandler numericHandler = new NumericHandler();

    public void parseNumericValForMinMax(Map<String, Object[]> attributeManMaxValues, //
            Object obj, String fieldName) {
        Object objVal = obj;
        if (!attributeManMaxValues.containsKey(fieldName)) {
            attributeManMaxValues.put(fieldName, null);
        }

        Object[] fieldBucketMap = attributeManMaxValues.get(fieldName);
        if (fieldBucketMap == null) {
            fieldBucketMap = new Object[2];
            fieldBucketMap[0] = objVal;
            fieldBucketMap[1] = objVal;
            attributeManMaxValues.put(fieldName, fieldBucketMap);
        }

        Object bucketMin = fieldBucketMap[0];
        Object bucketMax = fieldBucketMap[1];

        if (isGreater(bucketMin, objVal)) {
            fieldBucketMap[0] = objVal;
        } else if (isGreater(objVal, bucketMax)) {
            fieldBucketMap[1] = objVal;
        }
    }

    public void parseAttribute(Map<FundamentalType, List<String>> typeFieldMap, List<Integer> encodedColumnsPos,
            Map<String, Map<String, Long>> attributeValueBuckets, Map<String, List<String>> bucketLblOrderMap,
            Map<String, List<Object>> bucketOrderMap, Map<String, Map<String, Long[]>> binaryCodedBuckets,
            Map<String, List<Object>> minMaxInfo, int i, Object obj, String fieldName, int maxBucketCount,
            String encodedNoKey, String encodedYesKey, boolean numericalBucketsRequired,
            Map<String, Map<String, Long>> nAttributeBucketIds) {

        if (obj instanceof Boolean) {
            Boolean objVal = (Boolean) obj;
            booleanHandler.handleBooleanAttribute(attributeValueBuckets, fieldName, objVal, nAttributeBucketIds);
        } else if (obj instanceof Long //
                || obj instanceof Integer //
                || obj instanceof Double) {
            if (numericalBucketsRequired) {
                if (minMaxInfo != null && minMaxInfo.get(fieldName) != null)
                    numericHandler.handleNumericalAttribute(attributeValueBuckets, obj, fieldName, bucketLblOrderMap,
                            minMaxInfo.get(fieldName), bucketOrderMap, maxBucketCount, nAttributeBucketIds);
            }
        } else {
            List<String> booleanFields = typeFieldMap.get(FundamentalType.BOOLEAN);
            if (booleanFields.contains(fieldName)) {
                String objVal = (String) obj;
                booleanTextHandler.handleBooleanTextAttribute(attributeValueBuckets, fieldName, objVal,
                        nAttributeBucketIds);
            } else if (encodedColumnsPos.contains(i)) {
                encodedAttributeHandler.handleEncodedAttribute(binaryCodedBuckets, obj, fieldName, encodedNoKey,
                        encodedYesKey, nAttributeBucketIds);
            }
        }
    }

    private boolean isGreater(Object firstVal, Object secondVal) {
        boolean isGreater = false;
        if (secondVal instanceof Long) {
            isGreater = ((Long) firstVal) > ((Long) secondVal);
        } else if (secondVal instanceof Integer) {
            isGreater = ((Integer) firstVal) > ((Integer) secondVal);
        } else if (secondVal instanceof Double) {
            isGreater = ((Double) firstVal) > ((Double) secondVal);
        }
        return isGreater;
    }

}