package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.io.IOException;
import java.io.Serializable;

import org.apache.avro.util.Utf8;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.dataflow.runtime.cascading.propdata.util.stats.bucket.AttrStatsDetailsMergeFactory;
import com.latticeengines.dataflow.runtime.cascading.propdata.util.stats.bucket.AttrStatsDetailsMergeFactory.MergeType;
import com.latticeengines.dataflow.runtime.cascading.propdata.util.stats.bucket.AttrStatsDetailsMergeTool;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;

import cascading.tuple.Tuple;

public class AMStatsDimensionUtil implements Serializable {

    private static final long serialVersionUID = -4077418684968437466L;
    private static final Log log = LogFactory.getLog(AMStatsDimensionUtil.class);
    private static final ObjectMapper OM = new ObjectMapper();

    private AttrStatsDetailsMergeTool dedupMergeUtil = //
            AttrStatsDetailsMergeFactory.getUtil(MergeType.DEDUP);
    private AttrStatsDetailsMergeTool addMergeUtil = //
            AttrStatsDetailsMergeFactory.getUtil(MergeType.ADD);

    public ExpandedTuple dedup(ExpandedTuple existingMergedTuple, //
            ExpandedTuple originalTuple, int numOfFields) {
        return mergeWithTool(existingMergedTuple, originalTuple, numOfFields, dedupMergeUtil);
    }

    public ExpandedTuple merge(ExpandedTuple existingMergedTuple, //
            ExpandedTuple originalTuple, int numOfFields) {
        return mergeWithTool(existingMergedTuple, originalTuple, numOfFields, addMergeUtil);
    }

    private ExpandedTuple mergeWithTool(ExpandedTuple existingMergedTuple, //
                                ExpandedTuple originalTuple, int numOfFields, AttrStatsDetailsMergeTool tool) {
        for (int idx = 0; idx < numOfFields; idx++) {
            Object objInExistingMergedTuple = existingMergedTuple.get(idx);
            Object objInOriginalTuple = originalTuple.get(idx);
            if (objInExistingMergedTuple == null) {
                existingMergedTuple.set(idx, objInOriginalTuple);
            } else if (objInOriginalTuple == null) {
                existingMergedTuple.set(idx, objInExistingMergedTuple);
            } else if (objInExistingMergedTuple instanceof AttributeStats) {
                Object finalObj = objInExistingMergedTuple;
                if (objInOriginalTuple instanceof AttributeStats) {
                    finalObj = tool.merge(//
                            (AttributeStats) objInExistingMergedTuple, (AttributeStats) objInOriginalTuple, false);
                }
                existingMergedTuple.set(idx, finalObj);
            }
        }

        return existingMergedTuple;

    }

    public static class ExpandedTuple {
        Object[] attrValuesArr;

        ExpandedTuple(ExpandedTuple expandedTuple) {
            // used for deep copy during stats calculation
            this.attrValuesArr = new Object[expandedTuple.attrValuesArr.length];

            for (int idx = 0; idx < attrValuesArr.length; idx++) {
                Object objInOriginalTuple = expandedTuple.get(idx);

                if (objInOriginalTuple != null) {
                    if (objInOriginalTuple instanceof AttributeStats) {
                        attrValuesArr[idx] = new AttributeStats((AttributeStats) objInOriginalTuple);
                    } else {
                        attrValuesArr[idx] = objInOriginalTuple;
                    }
                } else {
                    attrValuesArr[idx] = null;
                }
            }
        }

        ExpandedTuple(Tuple tuple) {
            this.attrValuesArr = new Object[tuple.size()];

            for (int idx = 0; idx < attrValuesArr.length; idx++) {
                Object objInOriginalTuple = tuple.getObject(idx);

                if (objInOriginalTuple != null
                        && (objInOriginalTuple instanceof String || objInOriginalTuple instanceof Utf8)) {
                    AttributeStats statsInOriginalTuple;
                    String strVal = objInOriginalTuple.toString();
                    if (strVal.startsWith("{") && strVal.endsWith("}")) {
                        try {
                            statsInOriginalTuple = OM.readValue(objInOriginalTuple.toString(), AttributeStats.class);
                            attrValuesArr[idx] = statsInOriginalTuple;
                        } catch (IOException e) {
                            // ignore if type of serialized obj is not
                            // statsInExistingMergedTuple
                            log.info(String.format(
                                    "Got deserialization exception=%s, Ignoring this exception and using original "
                                            + "value [%s] as it could not be deserialized to AttributeStats object",
                                    e.getMessage(), objInOriginalTuple), e);
                            attrValuesArr[idx] = objInOriginalTuple;
                        }
                    } else {
                        attrValuesArr[idx] = strVal;
                    }
                } else {
                    attrValuesArr[idx] = objInOriginalTuple;
                }
            }
        }

        public Object get(int idx) {
            return attrValuesArr[idx];
        }

        public void set(int idx, Object objInOriginalTuple) {
            attrValuesArr[idx] = objInOriginalTuple;
        }

        int size() {
            return attrValuesArr.length;
        }

        Tuple generateTuple() {
            Tuple tuple = Tuple.size(attrValuesArr.length);

            for (int idx = 0; idx < attrValuesArr.length; idx++) {
                Object value = attrValuesArr[idx];
                if (value != null && value instanceof AttributeStats) {
                    try {
                        value = OM.writeValueAsString(value);
                    } catch (JsonProcessingException e) {
                        // cannot output AttributeStats to avro
                        throw new RuntimeException(String.format(
                                "Got serialization exception=%s, Ignoring this exception and using original "
                                        + "value [%s] of type %s instead of its serialized format",
                                e.getMessage(), value, value.getClass().getName()), e);
                    }
                }
                tuple.set(idx, value);
            }

            if (tuple.size() != attrValuesArr.length) {
                throw new RuntimeException("The size of generated tuple is " + tuple.size() + ", but there are "
                        + attrValuesArr.length + " in attrValuesArr.");
            }

            return tuple;
        }
    }
}
