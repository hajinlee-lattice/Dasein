package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataflow.runtime.cascading.propdata.util.stats.StatsAttributeParser;
import com.latticeengines.dataflow.runtime.cascading.propdata.util.stats.handlers.BooleanTextHandler;
import com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStatsDetails;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.statistics.BucketType;
import com.latticeengines.domain.exposed.datacloud.statistics.Buckets;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.FundamentalType;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class AMStatsLeafFieldSubstitutionFunction extends BaseOperation<Map> //
        implements Function<Map> {
    private static final long serialVersionUID = -4039806083023012431L;
    private static BooleanTextHandler booleanTextHandler = new BooleanTextHandler();

    private static ObjectMapper OM = new ObjectMapper();

    private volatile Map<String, List<Object>> minMaxInfo = null;

    private Map<String, Integer> namePositionMap;
    private Map<String, FundamentalType> fieldFundamentalTypeMap;
    private Map<FundamentalType, List<String>> typeFieldMap;

    private List<String> minMaxAndDimensionList;
    private List<Integer> encodedColumnsPos;

    private Set<String> dimensionSet;

    private int maxBucketCount;

    private boolean numericalBucketsRequired;

    private String encodedNo;
    private String encodedYes;
    private String minMaxKey;
    private String tempRenamedPrefix;

    public AMStatsLeafFieldSubstitutionFunction(Params params) {
        super(params.outputFieldsDeclaration);

        minMaxKey = params.minMaxKey;
        numericalBucketsRequired = params.numericalBucketsRequired;
        maxBucketCount = params.maxBucketCount;
        encodedNo = params.encodedNo;
        encodedYes = params.encodedYes;
        tempRenamedPrefix = params.tempRenamedPrefix;
        typeFieldMap = params.typeFieldMap;

        namePositionMap = new HashMap<>();

        for (int i = 0; i < params.fieldNames.size(); i++) {
            namePositionMap.put(params.fieldNames.get(i), i);
        }

        minMaxAndDimensionList = new ArrayList<>();
        dimensionSet = new HashSet<>();

        for (FieldMetadata fieldMeta : params.minMaxAndDimensionList) {
            minMaxAndDimensionList.add(fieldMeta.getFieldName());
            if (fieldMeta.getFieldName().equals(minMaxKey)) {
                continue;
            }
            dimensionSet.add(fieldMeta.getFieldName());
        }

        encodedColumnsPos = new ArrayList<>();
        for (String enCol : params.encodedColumns) {
            encodedColumnsPos.add(namePositionMap.get(enCol));
        }

        fieldFundamentalTypeMap = new HashMap<>();
        for (FundamentalType type : params.typeFieldMap.keySet()) {
            for (String fieldName : params.typeFieldMap.get(type)) {
                fieldFundamentalTypeMap.put(fieldName, type);
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void operate(FlowProcess flowProcess, FunctionCall<Map> functionCall) {
        Map<String, Object> dimensionFieldValuesMap = new HashMap<>();

        TupleEntry entry = functionCall.getArguments();

        Tuple result = Tuple.size(getFieldDeclaration().size());

        Fields fields = entry.getFields();
        Iterator<Object> it = fields.iterator();

        while (it.hasNext()) {
            String fieldName = (String) it.next();

            if (fieldName.equals(minMaxKey)) {
                Object val = entry.getObject(fieldName);
                dimensionFieldValuesMap.put(fieldName, val);
            } else if (dimensionSet.contains(fieldName)) {
                Object val = entry.getObject(fieldName);
                dimensionFieldValuesMap.put(fieldName, val);
            }
        }

        StatsAttributeParser attributeParser = new StatsAttributeParser();

        if (numericalBucketsRequired && minMaxInfo == null) {
            minMaxInfo = parseMinMaxInfo(entry);
        }

        int idx = 0;
        Iterator<Object> itr = fields.iterator();
        while (itr.hasNext()) {
            String fieldName = (String) itr.next();
            Object val = entry.getObject(fieldName);
            if (fieldName.equals(minMaxKey)) {
                dimensionFieldValuesMap.put(fieldName, val);
            } else if (dimensionSet.contains(fieldName)) {
                dimensionFieldValuesMap.put(fieldName, val);
            } else {
                parseField(entry, result, idx++, attributeParser, //
                        minMaxInfo, fieldFundamentalTypeMap, fieldName, val);
            }
        }

        updateResult(result, dimensionFieldValuesMap, functionCall.getDeclaredFields());
        functionCall.getOutputCollector().add(result);
    }

    private void parseField(TupleEntry entry, Tuple result, int pos, StatsAttributeParser attributeParser,
            Map<String, List<Object>> minMaxInfo, Map<String, FundamentalType> fieldFundamentalTypeMap,
            String fieldName, Object val) {

        boolean isEncodedAttr = isEncodedAttr(pos);

        if (val != null) {
            if ((minMaxInfo != null) && minMaxInfo.containsKey(fieldName)) {
                parseNumericField(result, pos, attributeParser, minMaxInfo, fieldName, val);
            } else if (isBooleanField(fieldFundamentalTypeMap, fieldName, val, isEncodedAttr)) {
                parseBooleanField(result, pos, attributeParser, minMaxInfo, fieldName, val, isEncodedAttr);
            } else {
                setDefaultStats(result, pos, fieldName, val);
            }
        } else {
            setDefaultStats(result, pos, fieldName, null);
        }

    }

    private void parseBooleanField(Tuple result, int pos, StatsAttributeParser attributeParser,
            Map<String, List<Object>> minMaxInfo, String fieldName, Object val, boolean isEncodedAttr) {

        AttributeStatsDetails attrStatsDetails = new AttributeStatsDetails();
        Buckets buckets = new Buckets();
        List<Bucket> bucketList = new ArrayList<>();
        Long count = 0L;

        Map<String, Map<String, Long>> attributeValueBuckets = new HashMap<>();
        Map<String, Map<String, Long[]>> binaryCodedBuckets = new HashMap<>();

        Map<String, Map<String, Long>> nAttributeBucketIds = new HashMap<>();
        attributeParser.parseAttribute(typeFieldMap, encodedColumnsPos, attributeValueBuckets, null, null,
                binaryCodedBuckets, minMaxInfo, pos, val, fieldName, maxBucketCount, encodedNo, encodedYes,
                numericalBucketsRequired, nAttributeBucketIds);

        if (isRegularBooleanField(attributeValueBuckets, fieldName, isEncodedAttr)) {
            for (String lbl : attributeValueBuckets.get(fieldName).keySet()) {
                count = createRegularBucket(attributeValueBuckets, fieldName, //
                        nAttributeBucketIds, bucketList, count, lbl);
            }
        } else if (isEncodedAttr) {
            for (String lbl : binaryCodedBuckets.get(fieldName).keySet()) {
                count = setEncodedBooleanBucket(binaryCodedBuckets, fieldName, //
                        bucketList, count, lbl);
            }
        }

        buckets.setBucketList(bucketList);
        buckets.setType(BucketType.Boolean);

        if (val != null) {
            count = 1L;
        }

        attrStatsDetails.setNonNullCount(count);
        attrStatsDetails.setBuckets(buckets);

        setStatsForField(result, pos, fieldName, attrStatsDetails);
    }

    private void parseNumericField(Tuple result, int pos, StatsAttributeParser attributeParser,
            Map<String, List<Object>> minMaxInfo, String fieldName, Object val) {

        Map<String, Map<String, Long>> nAttributeValueBuckets = new HashMap<>();
        Map<String, Map<String, Long>> nAttributeBucketIds = new HashMap<>();

        attributeParser.parseAttribute(typeFieldMap, encodedColumnsPos, //
                nAttributeValueBuckets, null, null, null, minMaxInfo, pos, val, //
                fieldName, maxBucketCount, encodedNo, //
                encodedYes, numericalBucketsRequired, nAttributeBucketIds);

        AttributeStatsDetails attrStatsDetails = new AttributeStatsDetails();
        Buckets buckets = new Buckets();
        List<Bucket> bucketList = new ArrayList<>();
        Long count = 0L;
        if (nAttributeValueBuckets.get(fieldName) != null//
                && !nAttributeValueBuckets.get(fieldName).isEmpty()) {
            for (String lbl : nAttributeValueBuckets.get(fieldName).keySet()) {
                count = createRegularBucket(nAttributeValueBuckets, fieldName, //
                        nAttributeBucketIds, bucketList, count, lbl);
            }

            buckets.setBucketList(bucketList);
            buckets.setType(BucketType.Numerical);

            if (val != null) {
                count = 1L;
            }

            attrStatsDetails.setNonNullCount(count);
            attrStatsDetails.setBuckets(buckets);
        }

        setStatsForField(result, pos, fieldName, attrStatsDetails);
    }

    private void setDefaultStats(Tuple result, int pos, String fieldName, Object val) {

        AttributeStatsDetails attrStatsDetails = new AttributeStatsDetails();
        Long count = 0L;
        if (val != null) {
            count = 1L;
        }
        attrStatsDetails.setNonNullCount(count);

        setStatsForField(result, pos, fieldName, attrStatsDetails);
    }

    private boolean isBooleanField(Map<String, FundamentalType> fieldFundamentalTypeMap, String fieldName, Object val,
            boolean isEncodedAttr) {

        return val instanceof Boolean //
                || (fieldFundamentalTypeMap.get(fieldName) == FundamentalType.BOOLEAN) //
                || isEncodedAttr;
    }

    private boolean isEncodedAttr(int pos) {

        boolean isEncodedAttr = false;
        for (int encodedColPos : encodedColumnsPos) {
            if (pos == encodedColPos) {
                isEncodedAttr = true;
                break;
            }
        }
        return isEncodedAttr;
    }

    private void updateResult(Tuple result, Map<String, Object> dimensionFieldValuesMap, //
            Fields fields) {

        for (String dimension : minMaxAndDimensionList) {
            if (!dimension.equals(minMaxKey)) {
                Object dimensionId = dimensionFieldValuesMap.get(dimension);
                int pos = fields.getPos(tempRenamedPrefix + dimension);
                result.set(pos, dimensionId);
            }
        }
    }

    private Map<String, List<Object>> parseMinMaxInfo(TupleEntry entry) {
        Map<String, List<Object>> minMaxInfo = new HashMap<>();
        String minMaxObjStr = entry.getString(minMaxKey);

        if (minMaxObjStr != null) {
            Map<?, ?> tempMinMaxInfoMap1 = JsonUtils.deserialize(minMaxObjStr, Map.class);
            Map<String, List> tempMinMaxInfoMap2 = //
                    JsonUtils.convertMap(tempMinMaxInfoMap1, String.class, List.class);
            minMaxInfo = new HashMap<>();
            for (String key : tempMinMaxInfoMap2.keySet()) {
                List<Object> minMaxList = //
                        JsonUtils.convertList(tempMinMaxInfoMap2.get(key), Object.class);
                minMaxInfo.put(key, minMaxList);
            }
        }
        return minMaxInfo;
    }

    private boolean isRegularBooleanField(Map<String, Map<String, Long>> attributeValueBuckets, //
            String fieldName, boolean isEncodedAttr) {

        return !isEncodedAttr && attributeValueBuckets.get(fieldName) != null
                && !attributeValueBuckets.get(fieldName).isEmpty();
    }

    private void setStatsForField(Tuple result, int pos, String fieldName, //
            AttributeStatsDetails attrStatsDetails) {

        try {
            result.set(pos, OM.writeValueAsString(attrStatsDetails));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(fieldName, e);
        }
    }

    private Long setEncodedBooleanBucket(Map<String, Map<String, Long[]>> binaryCodedBuckets, //
            String fieldName, List<Bucket> bucketList, Long count, String lbl) {

        Bucket bucket = new Bucket();
        bucket.setBucketLabel(lbl);
        bucket.setId(booleanTextHandler.assignIdForBooleanText(lbl));
        Long[] encodedCountArr = binaryCodedBuckets.get(fieldName).get(lbl);
        bucket.setEncodedCountList(encodedCountArr);
        if (encodedCountArr != null && encodedCountArr.length > 0) {
            count += encodedCountArr[0];
        }
        bucketList.add(bucket);
        return count;
    }

    private Long createRegularBucket(Map<String, Map<String, Long>> attributeValueBuckets, //
            String fieldName, Map<String, Map<String, Long>> nAttributeBucketIds, //
            List<Bucket> bucketList, Long count, String lbl) {

        Bucket bucket = new Bucket();
        bucket.setBucketLabel(lbl);
        bucket.setId(nAttributeBucketIds.get(fieldName).get(lbl));
        bucket.setCount(attributeValueBuckets.get(fieldName).get(lbl));
        count += attributeValueBuckets.get(fieldName).get(lbl);
        bucketList.add(bucket);
        return count;
    }

    public static class Params {
        Fields outputFieldsDeclaration;
        String encodedYes;
        String encodedNo;
        String tempRenamedPrefix;
        String minMaxKey;
        int maxBucketCount;
        Map<FundamentalType, List<String>> typeFieldMap;
        List<String> encodedColumns;
        boolean numericalBucketsRequired;
        List<FieldMetadata> minMaxAndDimensionList;
        List<String> fieldNames;

        public Params(Fields outputFieldsDeclaration, //
                String tempRenamedPrefix, String minMaxKey, //
                AccountMasterStatsParameters statsParameters, //
                List<FieldMetadata> minMaxAndDimensionList, List<String> fieldNames) {
            this.outputFieldsDeclaration = outputFieldsDeclaration;
            this.encodedYes = AccountMasterStatsParameters.ENCODED_YES;
            this.encodedNo = AccountMasterStatsParameters.ENCODED_NO;
            this.tempRenamedPrefix = tempRenamedPrefix;
            this.minMaxKey = minMaxKey;
            this.maxBucketCount = statsParameters.getMaxBucketCount();
            this.typeFieldMap = statsParameters.getTypeFieldMap();
            this.encodedColumns = statsParameters.getEncodedColumns();
            this.numericalBucketsRequired = statsParameters.isNumericalBucketsRequired();
            this.minMaxAndDimensionList = minMaxAndDimensionList;
            this.fieldNames = fieldNames;
        }

    }
}
