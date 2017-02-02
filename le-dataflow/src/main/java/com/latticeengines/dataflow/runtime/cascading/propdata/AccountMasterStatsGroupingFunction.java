package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.JsonUtils;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings({ "rawtypes", "serial" })
public class AccountMasterStatsGroupingFunction extends BaseOperation implements Buffer {
    private static final Log log = LogFactory.getLog(AccountMasterStatsGroupingFunction.class);

    protected Map<String, Integer> namePositionMap;

    private int maxAttrs;

    private Integer totalLoc;

    private String minMaxKey;

    private int maxBucketCount;

    private Map<Comparable, Integer> attrIdMap;

    private String[] dimensionIdFieldNames;

    private String[] attrFields;

    public String lblOrderPost;
    public String lblOrderPreNumeric;
    public String lblOrderPreBoolean;
    public String countKey;

    private ObjectMapper om = new ObjectMapper();

    public AccountMasterStatsGroupingFunction(Params parameterObject) {
        super(parameterObject.fieldDeclaration);
        maxAttrs = parameterObject.attrFields.length;

        namePositionMap = new HashMap<>();
        dimensionIdFieldNames = parameterObject.dimensionIdFieldNames;
        minMaxKey = parameterObject.minMaxKey;
        maxBucketCount = parameterObject.maxBucketCount;
        lblOrderPost = parameterObject.lblOrderPost;
        lblOrderPreNumeric = parameterObject.lblOrderPreNumeric;
        lblOrderPreBoolean = parameterObject.lblOrderPreBoolean;
        countKey = parameterObject.countKey;

        attrFields = parameterObject.attrFields;

        for (int i = 0; i < parameterObject.attrFields.length; i++) {
            namePositionMap.put(parameterObject.attrs[i], parameterObject.attrIds[i]);
        }
        namePositionMap.put(parameterObject.totalField, parameterObject.attrs.length);
        this.totalLoc = namePositionMap.get(parameterObject.totalField);

        attrIdMap = new HashMap<Comparable, Integer>();

        for (int i = 0; i < parameterObject.attrs.length; i++) {
            Integer attrId = parameterObject.attrIds[i];
            if (attrId >= maxAttrs) {
                log.info("Skip attribute " + parameterObject.attrs[i] + " for invalid id " + attrId);
                continue;
            }
            attrIdMap.put(parameterObject.attrs[i], attrId);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        Tuple result = Tuple.size(getFieldDeclaration().size());

        TupleEntry group = bufferCall.getGroup();
        setupTupleForGroup(result, group);
        Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();
        long[] attrCount = new long[maxAttrs];
        Map<String, Map<String, Long>> attributeValueBuckets = new HashMap<>();
        Map<String, List<String>> bucketLblOrderMap = new HashMap<>();
        Map<String, List<Object>> bucketOrderMap = new HashMap<>();

        long totalCount = countAttrsForArgument(arguments, attrCount, //
                attributeValueBuckets, bucketLblOrderMap, bucketOrderMap);

        for (int i = 0; i < attrCount.length - dimensionIdFieldNames.length - 1; i++) {
            Map<String, Long> stats = new HashMap<>();
            stats.put(countKey, attrCount[i]);
            if (attributeValueBuckets.get(attrFields[i]) != null) {
                Map<String, Long> valueBuckets = attributeValueBuckets.get(attrFields[i]);
                if (bucketLblOrderMap.get(attrFields[i]) != null) {
                    List<String> lblOrder = bucketLblOrderMap.get(attrFields[i]);
                    int order = 0;
                    for (String lbl : lblOrder) {
                        if (valueBuckets.get(lbl) != null) {
                            stats.put(lblOrderPreNumeric + (order++) + lblOrderPost + lbl, valueBuckets.get(lbl));
                        }
                    }
                } else if (valueBuckets.containsKey(Boolean.TRUE.toString())
                        || valueBuckets.containsKey(Boolean.FALSE.toString())) {
                    int pos = 0;
                    if (valueBuckets.get(Boolean.FALSE.toString()) != null) {
                        stats.put(lblOrderPreBoolean + (pos++) + lblOrderPost + Boolean.FALSE.toString(),
                                valueBuckets.get(Boolean.FALSE.toString()));
                    }
                    if (valueBuckets.get(Boolean.TRUE.toString()) != null) {
                        stats.put(lblOrderPreBoolean + (pos++) + lblOrderPost + Boolean.TRUE.toString(),
                                valueBuckets.get(Boolean.TRUE.toString()));
                    }
                }
            }

            try {
                result.setString(i, om.writeValueAsString(stats));
            } catch (JsonProcessingException e) {
                log.debug(e.getMessage(), e);
            }
        }

        result.setLong(totalLoc, totalCount);
        bufferCall.getOutputCollector().add(result);

    }

    private void setupTupleForGroup(Tuple result, TupleEntry group) {
        Fields fields = group.getFields();
        for (Object field : fields) {
            String fieldName = (String) field;
            Integer loc = namePositionMap.get(fieldName);
            if (loc != null && loc >= 0) {
                result.set(loc, group.getObject(fieldName));
            } else {
                log.error("Can not find field name=" + fieldName);
            }
        }
    }

    private long countAttrsForArgument(Iterator<TupleEntry> argumentsInGroup, long[] attrCount,
            Map<String, Map<String, Long>> attributeValueBuckets, Map<String, List<String>> bucketLblOrderMap,
            Map<String, List<Object>> bucketOrderMap) {
        int totalCount = 0;

        while (argumentsInGroup.hasNext()) {
            TupleEntry arguments = argumentsInGroup.next();
            Fields fields = arguments.getFields();
            int size = fields.size();
            String minMaxObjStr = arguments.getString(minMaxKey);
            Map<String, List<Object>> minMaxInfo = null;
            if (minMaxObjStr != null) {
                Map<?, ?> tempMinMaxInfoMap1 = JsonUtils.deserialize(minMaxObjStr, Map.class);
                Map<String, List> tempMinMaxInfoMap2 = JsonUtils.convertMap(tempMinMaxInfoMap1, String.class,
                        List.class);
                minMaxInfo = new HashMap<>();
                for (String key : tempMinMaxInfoMap2.keySet()) {
                    List<Object> minMaxList = JsonUtils.convertList(tempMinMaxInfoMap2.get(key), Object.class);
                    minMaxInfo.put(key, minMaxList);
                }
            }
            for (int i = 0; i < size; i++) {
                if (arguments.getObject(i) != null) {
                    Object obj = arguments.getObject(i);
                    String fieldName = fields.get(i).toString();
                    if (obj instanceof Boolean) {
                        Boolean objVal = (Boolean) obj;
                        if (!attributeValueBuckets.containsKey(fieldName)) {
                            attributeValueBuckets.put(fieldName, new HashMap<String, Long>());
                        }

                        Map<String, Long> fieldBucketMap = attributeValueBuckets.get(fieldName);
                        if (!fieldBucketMap.containsKey(objVal.toString())) {
                            fieldBucketMap.put(objVal.toString(), 0L);
                        }

                        Long bucketCount = fieldBucketMap.get(objVal.toString());
                        fieldBucketMap.put(objVal.toString(), ++bucketCount);
                    } else if (obj instanceof Long //
                            || obj instanceof Integer //
                            || obj instanceof Double) {
                        if (minMaxInfo != null && minMaxInfo.get(fieldName) != null)
                            parseNumericValForMinMax(attributeValueBuckets, obj, fieldName, bucketLblOrderMap,
                                    minMaxInfo.get(fieldName), bucketOrderMap);
                    }

                    Integer attrId = attrIdMap.get(fields.get(i));
                    if (attrId != null) {
                        attrCount[attrId]++;
                    }
                }
            }
            totalCount += 1;
        }
        return totalCount;
    }

    private void parseNumericValForMinMax(Map<String, Map<String, Long>> attributeValueBucket, Object obj,
            String fieldName, Map<String, List<String>> bucketLblOrderMap, List<Object> minMaxList,
            Map<String, List<Object>> bucketOrderMap) {
        Object objVal = obj;
        if (!attributeValueBucket.containsKey(fieldName)) {
            List<Object> buckets = getBuckets(obj, minMaxList);
            List<String> bucketLbls = getBucketLabels(buckets);

            attributeValueBucket.put(fieldName, new HashMap<String, Long>());
            bucketOrderMap.put(fieldName, buckets);
            bucketLblOrderMap.put(fieldName, bucketLbls);
        }

        List<Object> buckets = bucketOrderMap.get(fieldName);
        List<String> bucketLbls = bucketLblOrderMap.get(fieldName);

        Map<String, Long> fieldBucketMap = attributeValueBucket.get(fieldName);
        String bucketLbl = getMatchingBucketLbl(objVal, buckets, bucketLbls);

        if (!fieldBucketMap.containsKey(bucketLbl)) {
            fieldBucketMap.put(bucketLbl, 0L);
        }

        Long count = fieldBucketMap.get(bucketLbl);
        fieldBucketMap.put(bucketLbl, ++count);
    }

    private String getMatchingBucketLbl(Object objVal, List<Object> buckets, List<String> bucketLbls) {
        String lbl = null;

        if (objVal instanceof Long) {
            Long val = ((Long) objVal);
            for (int i = 0; i < buckets.size(); i++) {
                Long bucketA = (Long) buckets.get(i);

                if (val.longValue() < bucketA.longValue()) {

                } else if (val.equals(bucketA)) {
                    lbl = bucketLbls.get(i);
                    break;
                } else if (val.longValue() > bucketA.longValue()) {
                    if (i + 1 >= buckets.size()) {
                        lbl = bucketLbls.get(i);
                        break;
                    } else {
                        Long bucketB = (Long) buckets.get(i + 1);
                        if (val.longValue() < bucketB.longValue()) {
                            lbl = bucketLbls.get(i);
                            break;
                        }
                    }
                }
            }
        } else if (objVal instanceof Integer) {
            Integer val = ((Integer) objVal);
            for (int i = 0; i < buckets.size(); i++) {
                Integer bucketA = (Integer) buckets.get(i);

                if (val.intValue() < bucketA.intValue()) {

                } else if (val.equals(bucketA)) {
                    lbl = bucketLbls.get(i);
                    break;
                } else if (val.intValue() > bucketA.intValue()) {
                    if (i + 1 >= buckets.size()) {
                        lbl = bucketLbls.get(i);
                        break;
                    } else {
                        Integer bucketB = (Integer) buckets.get(i + 1);
                        if (val.intValue() < bucketB.intValue()) {
                            lbl = bucketLbls.get(i);
                            break;
                        }
                    }
                }
            }
        } else if (objVal instanceof Double) {
            Double val = ((Double) objVal);
            for (int i = 0; i < buckets.size(); i++) {
                Double bucketA = (Double) buckets.get(i);

                if (val.doubleValue() < bucketA.doubleValue()) {

                } else if (val.equals(bucketA)) {
                    lbl = bucketLbls.get(i);
                    break;
                } else if (val.doubleValue() > bucketA.doubleValue()) {
                    if (i + 1 >= buckets.size()) {
                        lbl = bucketLbls.get(i);
                        break;
                    } else {
                        Double bucketB = (Double) buckets.get(i + 1);
                        if (val.doubleValue() < bucketB.doubleValue()) {
                            lbl = bucketLbls.get(i);
                            break;
                        }
                    }
                }
            }
        }

        return lbl;
    }

    private List<String> getBucketLabels(List<Object> buckets) {
        List<String> bucketLbls = new ArrayList<>();
        for (int i = 0; i < buckets.size(); i++) {
            String lbl = calculateLabel(buckets, i);
            bucketLbls.add(lbl);
        }
        return bucketLbls;
    }

    private String calculateLabel(List<Object> buckets, int i) {
        String lbl = (i + 1 == buckets.size()) ? buckets.get(i) + "+"
                : buckets.get(i) + (getHigherPartOfLbl(buckets.get(i), buckets.get(i + 1)));
        return lbl;
    }

    private String getHigherPartOfLbl(Object lowerObject, Object higherObject) {
        String lblPart = "";
        if (higherObject instanceof Long) {
            Long obj = ((Long) higherObject) - 1;
            if (!(obj.compareTo((Long) lowerObject) == 0)) {
                lblPart = "-" + obj;
            }
        } else if (higherObject instanceof Integer) {
            Integer obj = ((Integer) higherObject) - 1;
            if (!(obj.compareTo((Integer) lowerObject) == 0)) {
                lblPart = "-" + obj;
            }
        } else if (higherObject instanceof Double) {
            Double obj = ((Double) higherObject);
            lblPart = "-" + obj;
        }
        return lblPart;
    }

    private List<Object> getBuckets(Object obj, List<Object> minMaxList) {
        List<Object> buckets = new ArrayList<>();
        if (obj instanceof Long) {
            Long min = 0L;
            Long max = 0L;

            if (minMaxList.get(0) instanceof Integer) {
                min = ((Integer) minMaxList.get(0)).longValue();
            } else {
                min = (Long) minMaxList.get(0);
            }
            if (minMaxList.get(1) instanceof Integer) {
                max = ((Integer) minMaxList.get(1)).longValue();
            } else {
                max = (Long) minMaxList.get(1);
            }

            Long diff = max - min;

            if (diff == 0) {
                buckets.add(min);
            } else if (diff <= maxBucketCount) {
                for (int i = 0; i < diff; i++) {
                    buckets.add(min + i);
                }
            } else {
                Long width = diff / maxBucketCount;
                for (int i = 0; i < maxBucketCount; i++) {
                    buckets.add(min + i * width);
                }
            }
        } else if (obj instanceof Integer) {
            Integer min = (Integer) minMaxList.get(0);
            Integer max = (Integer) minMaxList.get(1);

            if (minMaxList.get(0) instanceof Integer) {
                min = (Integer) minMaxList.get(0);
            } else {
                min = ((Long) minMaxList.get(0)).intValue();
            }
            if (minMaxList.get(1) instanceof Integer) {
                max = (Integer) minMaxList.get(1);
            } else {
                max = ((Long) minMaxList.get(0)).intValue();
            }

            Integer diff = max - min;

            if (diff == 0) {
                buckets.add(min);
            } else if (diff <= maxBucketCount) {
                for (int i = 0; i < diff; i++) {
                    buckets.add(min + i);
                }
            } else {
                Integer width = diff / maxBucketCount;
                for (int i = 0; i < maxBucketCount; i++) {
                    buckets.add(min + i * width);
                }
            }
        } else if (obj instanceof Double) {
            Double min = (Double) minMaxList.get(0);
            Double max = (Double) minMaxList.get(1);

            Double diff = max - min;

            if (diff == 0) {
                buckets.add(min);
            } else if (diff <= maxBucketCount) {
                for (int i = 0; i < diff; i++) {
                    buckets.add(min + i);
                }
            } else {
                Double width = diff / maxBucketCount;
                for (int i = 0; i < maxBucketCount; i++) {
                    buckets.add(min + i * width);
                }
            }
        }
        return buckets;
    }

    public static class Params {
        public String minMaxKey;
        public String[] attrs;
        public Integer[] attrIds;
        public Fields fieldDeclaration;
        public String[] attrFields;
        public String totalField;
        public String[] dimensionIdFieldNames;
        public int maxBucketCount;
        public String lblOrderPost;
        public String lblOrderPreNumeric;
        public String lblOrderPreBoolean;
        public String countKey;

        public Params(String minMaxKey, String[] attrs, Integer[] attrIds, Fields fieldDeclaration, String[] attrFields,
                String totalField, String[] dimensionIdFieldNames, int maxBucketCount, String lblOrderPost,
                String lblOrderPreNumeric, String lblOrderPreBoolean, String countKey) {
            super();
            this.minMaxKey = minMaxKey;
            this.attrs = attrs;
            this.attrIds = attrIds;
            this.fieldDeclaration = fieldDeclaration;
            this.attrFields = attrFields;
            this.totalField = totalField;
            this.dimensionIdFieldNames = dimensionIdFieldNames;
            this.maxBucketCount = maxBucketCount;
            this.lblOrderPost = lblOrderPost;
            this.lblOrderPreNumeric = lblOrderPreNumeric;
            this.lblOrderPreBoolean = lblOrderPreBoolean;
            this.countKey = countKey;
        }
    }
}
