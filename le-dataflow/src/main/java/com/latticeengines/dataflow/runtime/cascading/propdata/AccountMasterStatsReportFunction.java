package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.common.exposed.util.AMStatsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.statistics.AccountMasterCube;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStatistics;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStatsDetails;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.statistics.BucketType;
import com.latticeengines.domain.exposed.datacloud.statistics.Buckets;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import edu.emory.mathcs.backport.java.util.Arrays;

@SuppressWarnings("rawtypes")
public class AccountMasterStatsReportFunction extends BaseOperation implements Function {
    private static final Log log = LogFactory.getLog(AccountMasterStatsReportFunction.class);
    private static final long serialVersionUID = -2621164226184745299L;

    private static final int MaxColSize = 4096;
    private static final int MaxAttrCountSize = 32;

    private static final char EqualOp = '@';
    private static final char Delimiter = '|';

    private int cubeColumnPos;
    private Map<String, Long> rootIdsForNonRequiredDimensions;
    private List<String> leafSchemaNewColumnNames;
    private String groupTotalKey;
    private List<String> finalAttrColList;
    private String dimensionColumnPrepostfix;
    private String lblOrderPost;
    private String lblOrderPreEncodedYes;
    private String lblOrderPreEncodedNo;
    private String lblOrderPreNumeric;
    private String lblOrderPreBoolean;
    private String lblOrderPreObject;
    private String countKey;
    private String finalGroupTotalKey;

    private Integer[] attrLoc;
    private Map<String, Integer> namePositionMap;

    public AccountMasterStatsReportFunction(Params parameterObject) {
        super(parameterObject.fieldDeclaration);
        this.namePositionMap = getPositionMap(parameterObject.fieldDeclaration);

        leafSchemaNewColumnNames = new ArrayList<String>();
        this.groupTotalKey = parameterObject.groupTotalKey;
        this.finalAttrColList = parameterObject.finalAttrColList;
        this.rootIdsForNonRequiredDimensions = parameterObject.rootIdsForNonRequiredDimensions;
        this.dimensionColumnPrepostfix = parameterObject.dimensionColumnPrepostfix;
        this.lblOrderPost = parameterObject.lblOrderPost;
        this.lblOrderPreEncodedYes = parameterObject.lblOrderPreEncodedYes;
        this.lblOrderPreEncodedNo = parameterObject.lblOrderPreEncodedNo;
        this.lblOrderPreNumeric = parameterObject.lblOrderPreNumeric;
        this.lblOrderPreBoolean = parameterObject.lblOrderPreBoolean;
        this.lblOrderPreObject = parameterObject.lblOrderPreObject;
        this.countKey = parameterObject.countKey;
        this.finalGroupTotalKey = parameterObject.finalGroupTotalKey;

        int idx = 0;
        for (FieldMetadata metadata : parameterObject.leafSchemaNewColumns) {
            leafSchemaNewColumnNames.add(metadata.getFieldName());
            if (metadata.getFieldName().equals(parameterObject.cubeColumnName)) {
                cubeColumnPos = idx;
            }
            idx++;
        }

        this.attrLoc = new Integer[finalAttrColList.size()];
        int i = 0;
        for (String attrCol : finalAttrColList) {
            this.attrLoc[i++] = namePositionMap.get(attrCol);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {

        TupleEntry entry = functionCall.getArguments();

        Tuple tuple = entry.getTuple();
        Fields fields = entry.getFields();

        Tuple result = Tuple.size(leafSchemaNewColumnNames.size());

        Iterator<Object> itr = fields.iterator();
        int pos = 0;

        AccountMasterCube cube = new AccountMasterCube();
        Map<String, AttributeStatistics> statsList = new HashMap<>();
        cube.setStatistics(statsList);
        Map<String, Object> dimensionIds = new HashMap<>();

        StringBuilder[] attrBuilder = new StringBuilder[attrLoc.length];
        MutableInt curAttrCol = new MutableInt(0);

        StringBuilder builder = new StringBuilder(MaxColSize);
        attrBuilder[curAttrCol.intValue()] = builder;

        while (itr.hasNext()) {
            processRecord(tuple, itr, pos++, cube, statsList, dimensionIds, attrBuilder, curAttrCol);
        }

        try {
            for (String key : leafSchemaNewColumnNames) {
                String key1 = dimensionColumnPrepostfix + key + dimensionColumnPrepostfix;
                int pos1 = functionCall.getDeclaredFields().getPos(key);
                if (pos1 >= 0) {
                    result.set(pos1, dimensionIds.containsKey(key1) ? dimensionIds.get(key1)
                            : rootIdsForNonRequiredDimensions.get(key));
                }
            }
            result.set(cubeColumnPos, AMStatsUtils.compressAndEncode(cube));

            result.setLong(namePositionMap.get(finalGroupTotalKey), cube.getNonNullCount());

            for (int i = 0; i < attrLoc.length; i++) {
                if (attrBuilder[i] == null) {
                    break;
                }
                result.set(attrLoc[i], attrBuilder[i].toString());
            }

            functionCall.getOutputCollector().add(result);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void processRecord(Tuple tuple, Iterator<Object> itr, int pos, AccountMasterCube cube,
            Map<String, AttributeStatistics> statsList, Map<String, Object> dimensionIds, StringBuilder[] attrBuilder,
            MutableInt curAttrCol) {
        String field = (String) itr.next();
        Object value = tuple.getObject(pos);

        if (field.startsWith(dimensionColumnPrepostfix) //
                && field.endsWith(dimensionColumnPrepostfix)) {
            if (field.equals(groupTotalKey)) {
                cube.setNonNullCount((Long) value);
            } else {
                dimensionIds.put(field, value);
            }
            return;
        }

        if (value != null) {
            Map<String, Long[]> statsMap = //
                    JsonUtils.convertMap(//
                            JsonUtils.deserialize(value.toString(), Map.class), //
                            String.class, Long[].class);
            Long[] nonNullCount = statsMap.get(countKey);

            AttributeStatistics stats = new AttributeStatistics();
            AttributeStatsDetails rowBasedStatistics = new AttributeStatsDetails();
            rowBasedStatistics.setNonNullCount(nonNullCount[0]);
            getSplunkReportPortion(attrBuilder, curAttrCol, nonNullCount[0], pos);
            Buckets buckets = new Buckets();
            buckets.setType(BucketType.Numerical);
            Bucket[] bucketArray = null;
            if (statsMap.size() > 1) {
                bucketArray = new Bucket[statsMap.size() - 1];
            }

            for (String key : statsMap.keySet()) {
                if (key.equals(countKey)) {
                    continue;
                } else if (key.startsWith(lblOrderPreNumeric)) {
                    handleNumeric(statsMap, bucketArray, key);
                } else if (key.startsWith(lblOrderPreBoolean)) {
                    handleBoolean(statsMap, buckets, bucketArray, key);
                } else if (key.startsWith(lblOrderPreObject)) {
                    handleObject(statsMap, buckets, bucketArray, key);
                } else if (key.startsWith(lblOrderPreEncodedYes)) {
                    handleEncodedYes(statsMap, buckets, bucketArray, key);
                } else if (key.startsWith(lblOrderPreEncodedNo)) {
                    handleEncodedNo(statsMap, buckets, bucketArray, key);
                }
            }
            @SuppressWarnings("unchecked")
            List<Bucket> bucketList = (bucketArray == null)//
                    ? new ArrayList<Bucket>() //
                    : Arrays.asList(bucketArray);

            buckets.setBucketList(bucketList);
            rowBasedStatistics.setBuckets(buckets);
            stats.setRowBasedStatistics(rowBasedStatistics);
            statsList.put(field, stats);
        }
    }

    private void getSplunkReportPortion(StringBuilder[] attrBuilder, MutableInt curAttrCol, long count, int i) {
        StringBuilder builder = attrBuilder[curAttrCol.intValue()];
        builder.append(i);
        builder.append(EqualOp);
        builder.append(count);
        builder.append(Delimiter);
        if (builder.length() > (MaxColSize - MaxAttrCountSize)) {
            curAttrCol.increment();
            if (curAttrCol.intValue() >= attrLoc.length) {
                log.info("Number of attributes exceeds the max buffer size");
                return;
            }
            builder = new StringBuilder(MaxColSize);
            attrBuilder[curAttrCol.intValue()] = builder;
        }
    }

    private void handleEncodedNo(Map<String, Long[]> statsMap, Buckets buckets, Bucket[] bucketArray, String key) {
        String posPart = key.substring(lblOrderPreEncodedNo.length(), key.indexOf(lblOrderPost));
        String lbl = key.substring(key.indexOf(lblOrderPost) + 1);

        Bucket bucket = new Bucket();
        bucket.setBucketLabel(lbl);
        bucket.setEncodedCountList(statsMap.get(key));
        bucketArray[Integer.parseInt(posPart)] = bucket;
        buckets.setType(BucketType.Boolean);
    }

    private void handleEncodedYes(Map<String, Long[]> statsMap, Buckets buckets, Bucket[] bucketArray, String key) {
        String posPart = key.substring(lblOrderPreEncodedYes.length(), key.indexOf(lblOrderPost));
        String lbl = key.substring(key.indexOf(lblOrderPost) + 1);

        Bucket bucket = new Bucket();
        bucket.setBucketLabel(lbl);
        bucket.setEncodedCountList(statsMap.get(key));
        bucketArray[Integer.parseInt(posPart)] = bucket;
        buckets.setType(BucketType.Boolean);
    }

    private void handleObject(Map<String, Long[]> statsMap, Buckets buckets, Bucket[] bucketArray, String key) {
        String posPart = key.substring(lblOrderPreObject.length(), key.indexOf(lblOrderPost));
        String lbl = key.substring(key.indexOf(lblOrderPost) + 1);

        Bucket bucket = new Bucket();
        bucket.setBucketLabel(lbl);
        bucket.setCount(statsMap.get(key)[0]);
        bucketArray[Integer.parseInt(posPart)] = bucket;
        buckets.setType(BucketType.Boolean);
    }

    private void handleBoolean(Map<String, Long[]> statsMap, Buckets buckets, Bucket[] bucketArray, String key) {
        String posPart = key.substring(lblOrderPreBoolean.length(), key.indexOf(lblOrderPost));
        String lbl = key.substring(key.indexOf(lblOrderPost) + 1);

        Bucket bucket = new Bucket();
        bucket.setBucketLabel(lbl);
        bucket.setCount(statsMap.get(key)[0]);
        bucketArray[Integer.parseInt(posPart)] = bucket;
        buckets.setType(BucketType.Boolean);
    }

    private void handleNumeric(Map<String, Long[]> statsMap, Bucket[] bucketArray, String key) {
        String posPart = key.substring(lblOrderPreNumeric.length(), key.indexOf(lblOrderPost));
        String lbl = key.substring(key.indexOf(lblOrderPost) + 1);

        Bucket bucket = new Bucket();
        bucket.setBucketLabel(lbl);
        bucket.setCount(statsMap.get(key)[0]);
        bucketArray[Integer.parseInt(posPart)] = bucket;
    }

    private Map<String, Integer> getPositionMap(Fields fieldDeclaration) {
        Map<String, Integer> positionMap = new HashMap<>();
        int pos = 0;
        for (Object field : fieldDeclaration) {
            String fieldName = (String) field;
            positionMap.put(fieldName, pos++);
        }
        return positionMap;
    }

    public static class Params {
        public Fields fieldDeclaration;
        public List<FieldMetadata> leafSchemaNewColumns;
        public String cubeColumnName;
        public String groupTotalKey;
        public List<String> finalAttrColList;
        public Map<String, Long> rootIdsForNonRequiredDimensions;
        public String dimensionColumnPrepostfix;
        public String lblOrderPost;
        public String lblOrderPreEncodedYes;
        public String lblOrderPreEncodedNo;
        public String lblOrderPreNumeric;
        public String lblOrderPreBoolean;
        public String lblOrderPreObject;
        public String countKey;
        public String finalGroupTotalKey;

        public Params(Fields fieldDeclaration, List<FieldMetadata> leafSchemaNewColumns, String cubeColumnName,
                String groupTotalKey, List<String> finalAttrColList, Map<String, Long> rootIdsForNonRequiredDimensions,
                String dimensionColumnPrepostfix, String lblOrderPost, String lblOrderPreEncodedYes,
                String lblOrderPreEncodedNo, String lblOrderPreNumeric, String lblOrderPreBoolean,
                String lblOrderPreObject, String countKey, String finalGroupTotalKey) {
            super();
            this.fieldDeclaration = fieldDeclaration;
            this.leafSchemaNewColumns = leafSchemaNewColumns;
            this.cubeColumnName = cubeColumnName;
            this.groupTotalKey = groupTotalKey;
            this.finalAttrColList = finalAttrColList;
            this.rootIdsForNonRequiredDimensions = rootIdsForNonRequiredDimensions;
            this.dimensionColumnPrepostfix = dimensionColumnPrepostfix;
            this.lblOrderPost = lblOrderPost;
            this.lblOrderPreEncodedYes = lblOrderPreEncodedYes;
            this.lblOrderPreEncodedNo = lblOrderPreEncodedNo;
            this.lblOrderPreNumeric = lblOrderPreNumeric;
            this.lblOrderPreBoolean = lblOrderPreBoolean;
            this.lblOrderPreObject = lblOrderPreObject;
            this.countKey = countKey;
            this.finalGroupTotalKey = finalGroupTotalKey;
        }
    }
}
