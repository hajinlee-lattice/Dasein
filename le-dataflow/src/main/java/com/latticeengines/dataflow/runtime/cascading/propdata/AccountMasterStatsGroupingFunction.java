package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataflow.runtime.cascading.propdata.util.stats.StatsAttributeParser;
import com.latticeengines.domain.exposed.metadata.FundamentalType;

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

    private static final String ENCODED_NO = "NO";
    private static final String ENCODED_YES = "YES";
    private Map<String, Integer> namePositionMap;
    private int maxAttrs;
    private Integer totalLoc;
    private Params params;
    private Map<Comparable, Integer> attrIdMap;
    private List<Integer> encodedColumnsPos;

    public AccountMasterStatsGroupingFunction(Params parameterObject) {
        super(parameterObject.fieldDeclaration);

        this.params = parameterObject;

        maxAttrs = parameterObject.attrFields.length;

        namePositionMap = new HashMap<>();

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

        encodedColumnsPos = new ArrayList<>();
        for (String enCol : params.encodedColumns) {
            encodedColumnsPos.add(namePositionMap.get(enCol));
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        StatsAttributeParser attributeParser = new StatsAttributeParser();
        ObjectMapper om = new ObjectMapper();

        Tuple result = Tuple.size(getFieldDeclaration().size());

        TupleEntry group = bufferCall.getGroup();
        setupTupleForGroup(result, group);
        Iterator<TupleEntry> arguments = bufferCall.getArgumentsIterator();
        long[] attrCount = new long[maxAttrs];
        Map<String, Map<String, Long>> attributeValueBuckets = new HashMap<>();
        Map<String, List<String>> bucketLblOrderMap = new HashMap<>();
        Map<String, List<Object>> bucketOrderMap = new HashMap<>();
        Map<String, Map<String, Long[]>> binaryCodedBuckets = new HashMap<>();

        long totalCount = loopRecordsAndParseAttributes(attributeParser, arguments, //
                attrCount, attributeValueBuckets, bucketLblOrderMap, //
                bucketOrderMap, binaryCodedBuckets);

        for (int i = 0; i < attrCount.length - params.dimensionIdFieldNames.length - 1; i++) {
            Map<String, Long[]> stats = new HashMap<>();
            stats.put(params.countKey, new Long[] { attrCount[i] });

            if (encodedColumnsPos.contains(i)) {
                if (binaryCodedBuckets.get(params.attrFields[i]) != null //
                        && binaryCodedBuckets.get(params.attrFields[i]).size() > 0) {
                    stats.put(params.lblOrderPreEncodedYes + 0 + params.lblOrderPost + ENCODED_YES,
                            binaryCodedBuckets.get(params.attrFields[i]).get(ENCODED_YES));
                    stats.put(params.lblOrderPreEncodedNo + 1 + params.lblOrderPost + ENCODED_NO,
                            binaryCodedBuckets.get(params.attrFields[i]).get(ENCODED_NO));
                }
            } else if (attributeValueBuckets.get(params.attrFields[i]) != null) {
                Map<String, Long> valueBuckets = attributeValueBuckets.get(params.attrFields[i]);
                if (bucketLblOrderMap.get(params.attrFields[i]) != null) {
                    List<String> lblOrder = bucketLblOrderMap.get(params.attrFields[i]);
                    int order = 0;
                    for (String lbl : lblOrder) {
                        if (valueBuckets.get(lbl) != null) {
                            stats.put(params.lblOrderPreNumeric + (order++) + params.lblOrderPost + lbl,
                                    new Long[] { valueBuckets.get(lbl) });
                        }
                    }
                } else if (valueBuckets != null && (valueBuckets.containsKey(Boolean.TRUE.toString())
                        || valueBuckets.containsKey(Boolean.FALSE.toString()))) {
                    int pos = 0;
                    if (valueBuckets.get(Boolean.FALSE.toString()) != null) {
                        stats.put(params.lblOrderPreBoolean + (pos++) + params.lblOrderPost + Boolean.FALSE.toString(),
                                new Long[] { valueBuckets.get(Boolean.FALSE.toString()) });
                    }
                    if (valueBuckets.get(Boolean.TRUE.toString()) != null) {
                        stats.put(params.lblOrderPreBoolean + (pos++) + params.lblOrderPost + Boolean.TRUE.toString(),
                                new Long[] { valueBuckets.get(Boolean.TRUE.toString()) });
                    }
                } else if (valueBuckets != null && valueBuckets.size() > 0) {
                    int pos = 0;
                    List<String> lblList = new ArrayList<>();
                    for (String lbl : valueBuckets.keySet()) {
                        lblList.add(lbl);
                    }
                    Collections.sort(lblList);

                    for (String lbl : lblList) {
                        stats.put(
                                params.lblOrderPreObject + //
                                        (pos++) + params.lblOrderPost + lbl, //
                                new Long[] { valueBuckets.get(lbl) });
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

    private long loopRecordsAndParseAttributes(StatsAttributeParser attributeParser, //
            Iterator<TupleEntry> argumentsInGroup, long[] attrCount, //
            Map<String, Map<String, Long>> attributeValueBuckets, //
            Map<String, List<String>> bucketLblOrderMap, Map<String, //
            List<Object>> bucketOrderMap, //
            Map<String, Map<String, Long[]>> binaryCodedBuckets) {
        int totalCount = 0;

        while (argumentsInGroup.hasNext()) {
            TupleEntry arguments = argumentsInGroup.next();
            Fields fields = arguments.getFields();
            int size = fields.size();
            String minMaxObjStr = arguments.getString(params.minMaxKey);
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
                    attributeParser.parseAttribute(params.typeFieldMap, encodedColumnsPos, attributeValueBuckets,
                            bucketLblOrderMap, bucketOrderMap, binaryCodedBuckets, minMaxInfo, i, obj, fieldName,
                            params.maxBucketCount, ENCODED_NO, ENCODED_YES);

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

    public static class Params implements Serializable {
        String minMaxKey;
        String[] attrs;
        Integer[] attrIds;
        Fields fieldDeclaration;
        String[] attrFields;
        String totalField;
        String[] dimensionIdFieldNames;
        int maxBucketCount;
        String lblOrderPost;
        String lblOrderPreEncodedYes;
        String lblOrderPreEncodedNo;
        String lblOrderPreNumeric;
        String lblOrderPreBoolean;
        String lblOrderPreObject;
        String countKey;
        Map<FundamentalType, List<String>> typeFieldMap;
        List<String> encodedColumns;

        public Params(String minMaxKey, String[] attrs, Integer[] attrIds, Fields fieldDeclaration, String[] attrFields,
                String totalField, String[] dimensionIdFieldNames, int maxBucketCount, String lblOrderPost,
                String lblOrderPreEncodedYes, String lblOrderPreEncodedNo, String lblOrderPreNumeric,
                String lblOrderPreBoolean, String lblOrderPreObject, String countKey,
                Map<FundamentalType, List<String>> typeFieldMap, List<String> encodedColumns) {
            this.minMaxKey = minMaxKey;
            this.attrs = attrs;
            this.attrIds = attrIds;
            this.fieldDeclaration = fieldDeclaration;
            this.attrFields = attrFields;
            this.totalField = totalField;
            this.dimensionIdFieldNames = dimensionIdFieldNames;
            this.maxBucketCount = maxBucketCount;
            this.lblOrderPost = lblOrderPost;
            this.lblOrderPreEncodedYes = lblOrderPreEncodedYes;
            this.lblOrderPreEncodedNo = lblOrderPreEncodedNo;
            this.lblOrderPreNumeric = lblOrderPreNumeric;
            this.lblOrderPreBoolean = lblOrderPreBoolean;
            this.lblOrderPreObject = lblOrderPreObject;
            this.countKey = countKey;
            this.typeFieldMap = typeFieldMap;
            this.encodedColumns = encodedColumns;
        }
    }
}
