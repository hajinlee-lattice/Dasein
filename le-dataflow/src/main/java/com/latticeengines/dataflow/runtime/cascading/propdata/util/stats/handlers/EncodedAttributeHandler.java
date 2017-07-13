package com.latticeengines.dataflow.runtime.cascading.propdata.util.stats.handlers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.BitCodecUtils;

public class EncodedAttributeHandler {
    private static final Logger log = LoggerFactory.getLogger(EncodedAttributeHandler.class);

    public void handleEncodedAttribute(Map<String, Map<String, Long[]>> binaryCodedBuckets, Object obj,
            String fieldName, String encodedNoKey, String encodedYesKey, Map<String, Map<String, Long>> nAttributeBucketIds) {
        try {
            boolean[] decodedBooleanArray = BitCodecUtils.decodeAll((String) obj);

            if (binaryCodedBuckets.get(fieldName) == null) {
                initYesNoArrays(binaryCodedBuckets, fieldName, encodedNoKey, encodedYesKey, decodedBooleanArray);
            }

            Long[] yesCountArray = binaryCodedBuckets.get(fieldName).get(encodedYesKey);
            Long[] noCountArray = binaryCodedBuckets.get(fieldName).get(encodedNoKey);
            if (yesCountArray.length < decodedBooleanArray.length) {
                readjustYesNoArrays(binaryCodedBuckets, fieldName, encodedNoKey, encodedYesKey, decodedBooleanArray,
                        yesCountArray, noCountArray);

                yesCountArray = binaryCodedBuckets.get(fieldName).get(encodedYesKey);
                noCountArray = binaryCodedBuckets.get(fieldName).get(encodedNoKey);
            }

            int pos = 0;
            for (boolean flag : decodedBooleanArray) {
                if (flag) {
                    yesCountArray[pos] = yesCountArray[pos] + 1;
                } else {
                    noCountArray[pos] = noCountArray[pos] + 1;
                }
                pos++;
            }
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    private void readjustYesNoArrays(Map<String, Map<String, Long[]>> binaryCodedBuckets, String fieldName,
            String encodedNoKey, String encodedYesKey, boolean[] decodedBooleanArray, Long[] yesCountArray,
            Long[] noCountArray) {
        Long[] yesCountArrayNew = new Long[decodedBooleanArray.length];
        Long[] noCountArrayNew = new Long[decodedBooleanArray.length];

        for (int k = 0; k < decodedBooleanArray.length; k++) {
            yesCountArrayNew[k] = 0L;
            noCountArrayNew[k] = 0L;
        }

        for (int k = 0; k < yesCountArray.length; k++) {
            yesCountArrayNew[k] = yesCountArray[k];
            noCountArrayNew[k] = noCountArray[k];
        }

        binaryCodedBuckets.get(fieldName).put(encodedYesKey, yesCountArrayNew);
        binaryCodedBuckets.get(fieldName).put(encodedNoKey, noCountArrayNew);
    }

    private void initYesNoArrays(Map<String, Map<String, Long[]>> binaryCodedBuckets, String fieldName,
            String encodedNoKey, String encodedYesKey, boolean[] decodedBooleanArray) {
        Map<String, Long[]> map = new HashMap<>();
        binaryCodedBuckets.put(fieldName, map);
        Long[] yesCountArray = new Long[decodedBooleanArray.length];
        Long[] noCountArray = new Long[decodedBooleanArray.length];

        map.put(encodedYesKey, yesCountArray);
        map.put(encodedNoKey, noCountArray);

        for (int k = 0; k < decodedBooleanArray.length; k++) {
            yesCountArray[k] = 0L;
            noCountArray[k] = 0L;
        }
    }
}
