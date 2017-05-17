package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.io.IOException;
import java.io.Serializable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.dataflow.runtime.cascading.propdata.util.stats.bucket.AttrStatsDetailsMergeFactory;
import com.latticeengines.dataflow.runtime.cascading.propdata.util.stats.bucket.AttrStatsDetailsMergeFactory.MergeType;
import com.latticeengines.dataflow.runtime.cascading.propdata.util.stats.bucket.AttrStatsDetailsMergeTool;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStatsDetails;

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

        for (int idx = 0; idx < numOfFields; idx++) {
            Object objInExistingMergedTuple = existingMergedTuple.get(idx);
            Object objInOriginalTuple = originalTuple.get(idx);
            boolean isPrint = false;

            if (objInExistingMergedTuple == null) {
                existingMergedTuple.set(idx, objInOriginalTuple);
            } else if (objInOriginalTuple == null) {
                existingMergedTuple.set(idx, objInExistingMergedTuple);
            } else if (objInExistingMergedTuple instanceof AttributeStatsDetails) {
                Object finalObj = objInExistingMergedTuple;
                if (objInExistingMergedTuple instanceof AttributeStatsDetails
                        && objInOriginalTuple instanceof AttributeStatsDetails) {
                    finalObj = dedupMergeUtil.merge(//
                            (AttributeStatsDetails) objInExistingMergedTuple,
                            (AttributeStatsDetails) objInOriginalTuple, isPrint);
                }
                existingMergedTuple.set(idx, finalObj);
            }
        }

        return existingMergedTuple;
    }

    public ExpandedTuple merge(ExpandedTuple existingMergedTuple, //
            ExpandedTuple originalTuple, int numOfFields) {

        for (int idx = 0; idx < numOfFields; idx++) {
            Object objInExistingMergedTuple = existingMergedTuple.get(idx);
            Object objInOriginalTuple = originalTuple.get(idx);
            boolean isPrint = false;

            if (objInExistingMergedTuple == null) {
                existingMergedTuple.set(idx, objInOriginalTuple);
            } else if (objInOriginalTuple == null) {
                existingMergedTuple.set(idx, objInExistingMergedTuple);
            } else if (objInExistingMergedTuple instanceof AttributeStatsDetails) {
                Object finalObj = objInExistingMergedTuple;
                if (objInExistingMergedTuple instanceof AttributeStatsDetails
                        && objInOriginalTuple instanceof AttributeStatsDetails) {
                    finalObj = addMergeUtil.merge(//
                            (AttributeStatsDetails) objInExistingMergedTuple,
                            (AttributeStatsDetails) objInOriginalTuple, isPrint);
                }
                existingMergedTuple.set(idx, finalObj);
            }
        }

        return existingMergedTuple;
    }

    public static class ExpandedTuple {
        Object[] attrValuesArr;

        public ExpandedTuple(ExpandedTuple expandedTuple) {
            // used for deep copy during stats calculation
            attrValuesArr = new Object[expandedTuple.attrValuesArr.length];

            for (int idx = 0; idx < expandedTuple.attrValuesArr.length; idx++) {
                Object objInOriginalTuple = expandedTuple.get(idx);

                if (objInOriginalTuple != null) {
                    if (objInOriginalTuple instanceof AttributeStatsDetails) {
                        attrValuesArr[idx] = new AttributeStatsDetails(//
                                (AttributeStatsDetails) objInOriginalTuple);
                    } else if (objInOriginalTuple instanceof Long) {
                        attrValuesArr[idx] = new Long((Long) objInOriginalTuple);
                    } else if (objInOriginalTuple instanceof String) {

                        AttributeStatsDetails statsInOriginalTuple = null;

                        try {
                            statsInOriginalTuple = //
                                    OM.readValue((String) objInOriginalTuple, //
                                            AttributeStatsDetails.class);
                            attrValuesArr[idx] = statsInOriginalTuple;
                        } catch (IOException e) {
                            // ignore if type of serialized obj is not
                            // statsInExistingMergedTuple
                            log.debug(e.getMessage(), e);
                            attrValuesArr[idx] = objInOriginalTuple;

                        }
                    }
                } else {
                    attrValuesArr[idx] = objInOriginalTuple;
                }
            }
        }

        public ExpandedTuple(Tuple tuple, int fieldsLength) {
            attrValuesArr = new Object[fieldsLength];

            for (int idx = 0; idx < fieldsLength; idx++) {
                Object objInOriginalTuple = tuple.getObject(idx);

                if (objInOriginalTuple != null && objInOriginalTuple instanceof String) {
                    AttributeStatsDetails statsInOriginalTuple = null;

                    try {
                        statsInOriginalTuple = //
                                OM.readValue((String) objInOriginalTuple, //
                                        AttributeStatsDetails.class);
                        attrValuesArr[idx] = statsInOriginalTuple;
                    } catch (IOException e) {
                        // ignore if type of serialized obj is not
                        // statsInExistingMergedTuple
                        log.debug(e.getMessage(), e);
                        attrValuesArr[idx] = objInOriginalTuple;
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

        public Tuple generateTuple() {
            Tuple tuple = new Tuple();

            for (int idx = 0; idx < attrValuesArr.length; idx++) {
                Object value = attrValuesArr[idx];
                if (value != null && value instanceof AttributeStatsDetails) {
                    try {
                        value = OM.writeValueAsString(value);
                    } catch (JsonProcessingException e) {
                        log.debug(e.getMessage(), e);
                    }
                }
                tuple.add(value);
            }

            return tuple;
        }
    }
}
