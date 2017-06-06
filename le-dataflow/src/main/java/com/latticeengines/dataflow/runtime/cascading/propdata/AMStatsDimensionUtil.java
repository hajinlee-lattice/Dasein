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

        for (int idx = 0; idx < numOfFields; idx++) {
            Object objInExistingMergedTuple = existingMergedTuple.get(idx);
            Object objInOriginalTuple = originalTuple.get(idx);
            boolean isPrint = false;

            if (objInExistingMergedTuple == null) {
                existingMergedTuple.set(idx, objInOriginalTuple);
            } else if (objInOriginalTuple == null) {
                existingMergedTuple.set(idx, objInExistingMergedTuple);
            } else if (objInExistingMergedTuple instanceof AttributeStats) {
                Object finalObj = objInExistingMergedTuple;
                if (objInExistingMergedTuple instanceof AttributeStats
                        && objInOriginalTuple instanceof AttributeStats) {
                    finalObj = dedupMergeUtil.merge(//
                            (AttributeStats) objInExistingMergedTuple, (AttributeStats) objInOriginalTuple, isPrint);
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
            } else if (objInExistingMergedTuple instanceof AttributeStats) {
                Object finalObj = objInExistingMergedTuple;
                if (objInExistingMergedTuple instanceof AttributeStats
                        && objInOriginalTuple instanceof AttributeStats) {
                    finalObj = addMergeUtil.merge(//
                            (AttributeStats) objInExistingMergedTuple, (AttributeStats) objInOriginalTuple, isPrint);
                }
                existingMergedTuple.set(idx, finalObj);
            }
        }

        return existingMergedTuple;
    }

    public static class ExpandedTuple {
        Object[] attrValuesArr;
        int fieldsLength;

        public ExpandedTuple(ExpandedTuple expandedTuple) {
            // used for deep copy during stats calculation
            this.attrValuesArr = new Object[expandedTuple.fieldsLength];
            this.fieldsLength = expandedTuple.fieldsLength;

            checkZeroSizeTuple();

            for (int idx = 0; idx < expandedTuple.attrValuesArr.length; idx++) {
                Object objInOriginalTuple = expandedTuple.get(idx);

                if (objInOriginalTuple != null) {
                    if (objInOriginalTuple instanceof AttributeStats) {
                        attrValuesArr[idx] = new AttributeStats(//
                                (AttributeStats) objInOriginalTuple);
                    } else if (objInOriginalTuple instanceof Long) {
                        attrValuesArr[idx] = new Long((Long) objInOriginalTuple);
                    } else if (objInOriginalTuple instanceof String) {

                        AttributeStats statsInOriginalTuple = null;

                        try {
                            statsInOriginalTuple = //
                                    OM.readValue((String) objInOriginalTuple, //
                                            AttributeStats.class);
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
            this.attrValuesArr = new Object[fieldsLength];
            this.fieldsLength = fieldsLength;

            checkZeroSizeTuple();

            for (int idx = 0; idx < fieldsLength; idx++) {
                Object objInOriginalTuple = tuple.getObject(idx);

                if (objInOriginalTuple != null && objInOriginalTuple instanceof String) {
                    AttributeStats statsInOriginalTuple = null;

                    try {
                        statsInOriginalTuple = //
                                OM.readValue((String) objInOriginalTuple, //
                                        AttributeStats.class);
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

            checkZeroSizeTuple();

            Tuple tuple = new Tuple();

            for (int idx = 0; idx < attrValuesArr.length; idx++) {
                Object value = attrValuesArr[idx];
                if (value != null && value instanceof AttributeStats) {
                    try {
                        value = OM.writeValueAsString(value);
                    } catch (JsonProcessingException e) {
                        log.debug(e.getMessage(), e);
                    }
                }
                tuple.add(value);
            }

            if (tuple.size() == 0) {
                throw new RuntimeException("Got zero size for generated tuple");
            }

            return tuple;
        }

        private void checkZeroSizeTuple() {
            if (this.fieldsLength == 0) {
                throw new RuntimeException("Got zero size tuple");
            }
        }
    }
}
