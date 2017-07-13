package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.AMStatsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters;
import com.latticeengines.domain.exposed.datacloud.statistics.AccountMasterCube;
import com.latticeengines.domain.exposed.datacloud.statistics.AMAttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

@SuppressWarnings("rawtypes")
public class AMStatsReportFunction extends BaseOperation implements Function {
    private static final Logger log = LoggerFactory.getLogger(AMStatsReportFunction.class);
    private static final long serialVersionUID = -2621164226184745299L;

    private static final int MaxColSize = 4096;
    private static final int MaxAttrCountSize = 32;
    private static final char EqualOp = '@';
    private static final char Delimiter = '|';

    private int cubeColumnPos;
    private Map<String, Long> rootIdsForNonRequiredDimensions;
    private Map<String, Integer> namePositionMap;
    private Map<String, Integer> attrIdsMapForSplunk;

    private List<String> reportOutputColumnNames;
    private List<String> splunkReportColumns;

    private Integer[] splunkReportAttrLoc;

    private String finalGroupTotalKey;
    private String groupTotalKey;
    private String dimensionColumnPrepostfix;

    public AMStatsReportFunction(Params parameterObject) {
        super(parameterObject.reportOutputColumnFieldsDeclaration);
        this.namePositionMap = getPositionMap(parameterObject.reportOutputColumnFieldsDeclaration);
        this.reportOutputColumnNames = new ArrayList<String>();
        this.groupTotalKey = parameterObject.groupTotalKey;
        this.splunkReportColumns = parameterObject.splunkReportColumns;
        this.rootIdsForNonRequiredDimensions = parameterObject.rootIdsForNonRequiredDimensions;
        this.dimensionColumnPrepostfix = parameterObject.dimensionColumnPrepostfix;
        this.finalGroupTotalKey = parameterObject.finalGroupTotalKey;

        int idx = 0;
        for (FieldMetadata metadata : parameterObject.reportOutputColumns) {
            this.reportOutputColumnNames.add(metadata.getFieldName());

            if (metadata.getFieldName().equals(parameterObject.cubeColumnName)) {
                cubeColumnPos = idx;
            }
            idx++;
        }

        this.splunkReportAttrLoc = new Integer[splunkReportColumns.size()];
        idx = 0;
        for (String attrCol : splunkReportColumns) {
            this.splunkReportAttrLoc[idx++] = namePositionMap.get(attrCol);
        }

        attrIdsMapForSplunk = new HashMap<>();
        idx = 0;
        for (String attrName : parameterObject.columnsForStatsCalculation) {
            attrIdsMapForSplunk.put(attrName, //
                    parameterObject.columnIdsForStatsCalculation.get(idx++));
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {

        TupleEntry entry = functionCall.getArguments();

        Tuple tuple = entry.getTuple();
        Fields fields = entry.getFields();

        Tuple result = Tuple.size(reportOutputColumnNames.size());

        Iterator<Object> itr = fields.iterator();

        AccountMasterCube cube = new AccountMasterCube();
        Map<String, AMAttributeStats> statsList = new HashMap<>();
        cube.setStatistics(statsList);
        Map<String, Object> dimensionIds = new HashMap<>();

        StringBuilder[] attrBuilder = new StringBuilder[splunkReportAttrLoc.length];
        MutableInt curAttrCol = new MutableInt(0);

        StringBuilder builder = new StringBuilder(MaxColSize);
        attrBuilder[curAttrCol.intValue()] = builder;

        int pos = 0;

        while (itr.hasNext()) {
            String field = (String) itr.next();
            Object value = tuple.getObject(pos);

            processRecord(tuple, field, value, cube, //
                    statsList, dimensionIds, //
                    attrBuilder, curAttrCol);
            pos++;
        }

        try {
            setDimensionInfoInResult(functionCall, result, dimensionIds);

            result.setString(cubeColumnPos, AMStatsUtils.compressAndEncode(cube));

            result.setLong(namePositionMap.get(finalGroupTotalKey), cube.getNonNullCount());

            setSplunkReportInfoInResult(result, attrBuilder);

            functionCall.getOutputCollector().add(result);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void setSplunkReportInfoInResult(Tuple result, StringBuilder[] attrBuilder) {
        for (int i = 0; i < splunkReportAttrLoc.length; i++) {
            if (attrBuilder[i] == null) {
                break;
            }
            result.setString(splunkReportAttrLoc[i], attrBuilder[i].toString());
        }
    }

    private void setDimensionInfoInResult(FunctionCall functionCall, //
            Tuple result, Map<String, Object> dimensionIds) {
        for (String reportOutputCol : reportOutputColumnNames) {
            String reportOutputColWithPrePostFix = dimensionColumnPrepostfix + reportOutputCol
                    + dimensionColumnPrepostfix;
            int reportOutputColPos = functionCall.getDeclaredFields().getPos(reportOutputCol);
            if (isDimensionKey(dimensionIds, reportOutputCol, reportOutputColWithPrePostFix, reportOutputColPos)) {
                int dimensionKeyPos = reportOutputColPos;
                result.set(dimensionKeyPos,
                        dimensionIds.containsKey(reportOutputColWithPrePostFix)
                                ? dimensionIds.get(reportOutputColWithPrePostFix)
                                : rootIdsForNonRequiredDimensions.get(reportOutputCol));
            }
        }
    }

    private boolean isDimensionKey(Map<String, Object> dimensionIds, String reportOutputCol,
            String reportOutputColWithPrePostFix, int reportOutputColPos) {
        return reportOutputColPos >= 0 && (dimensionIds.containsKey(reportOutputColWithPrePostFix)
                || rootIdsForNonRequiredDimensions.containsKey(reportOutputCol));
    }

    private void processRecord(Tuple tuple, String field, Object value, //
                               AccountMasterCube cube, Map<String, AMAttributeStats> statsList, //
                               Map<String, Object> dimensionIds, StringBuilder[] attrBuilder, //
                               MutableInt curAttrCol) {

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
            AMAttributeStats stats = new AMAttributeStats();
            AttributeStats statsMap = //
                    JsonUtils.deserialize(value.toString(), AttributeStats.class);

            Long nonNullCount = statsMap.getNonNullCount();

            if (cube.getNonNullCount() == null) {
                cube.setNonNullCount(nonNullCount);
            } else if (nonNullCount != null && cube.getNonNullCount() < nonNullCount) {
                cube.setNonNullCount(nonNullCount);
            }

            if (attrIdsMapForSplunk.containsKey(field)) {
                getSplunkReportPortion(attrBuilder, curAttrCol, //
                        nonNullCount, //
                        attrIdsMapForSplunk.get(field));
            }

            sortBuckets(statsMap);

            stats.setRowBasedStatistics(statsMap);

            statsList.put(field, stats);
        }
    }

    private void sortBuckets(AttributeStats statsMap) {
        if (statsMap.getBuckets() != null //
                && CollectionUtils.isNotEmpty(statsMap.getBuckets().getBucketList()) //
                && statsMap.getBuckets().getBucketList().get(0).getId() != null) {
            List<Bucket> buckets = statsMap.getBuckets().getBucketList();
            List<Long> ids = new ArrayList<>();
            Map<Long, Bucket> idBucketMap = new HashMap<>();
            for (Bucket bucket : buckets) {
                if (bucket.getId() != null) {
                    ids.add(bucket.getId());
                    idBucketMap.put(bucket.getId(), bucket);
                }
            }
            Collections.sort(ids);
            buckets.clear();

            for (Long id : ids) {
                // put it back in sorted order
                buckets.add(idBucketMap.get(id));
            }
        }
    }

    private void getSplunkReportPortion(StringBuilder[] attrBuilder, //
            MutableInt curAttrCol, long count, int i) {
        StringBuilder builder = attrBuilder[curAttrCol.intValue()];
        builder.append(i);
        builder.append(EqualOp);
        builder.append(count);
        builder.append(Delimiter);
        if (builder.length() > (MaxColSize - MaxAttrCountSize)) {
            curAttrCol.increment();
            if (curAttrCol.intValue() >= splunkReportAttrLoc.length) {
                log.info("Number of attributes exceeds the max buffer size");
                return;
            }
            builder = new StringBuilder(MaxColSize);
            attrBuilder[curAttrCol.intValue()] = builder;
        }
    }

    private Map<String, Integer> getPositionMap(Fields reportOutputColumnFieldsDeclaration) {
        Map<String, Integer> positionMap = new HashMap<>();
        int pos = 0;
        for (Object field : reportOutputColumnFieldsDeclaration) {
            String fieldName = (String) field;
            positionMap.put(fieldName, pos++);
        }
        return positionMap;
    }

    public static class Params {
        public Fields reportOutputColumnFieldsDeclaration;
        public List<FieldMetadata> reportOutputColumns;
        public String cubeColumnName;
        public String groupTotalKey;
        public List<String> splunkReportColumns;
        public Map<String, Long> rootIdsForNonRequiredDimensions;
        public List<String> columnsForStatsCalculation;
        public List<Integer> columnIdsForStatsCalculation;
        public String dimensionColumnPrepostfix;
        public String finalGroupTotalKey;

        public Params(Fields reportOutputColumnFieldsDeclaration, //
                List<FieldMetadata> reportOutputColumns, //
                String groupTotalKey, //
                List<String> splunkReportColumns, //
                AccountMasterStatsParameters parameters) {
            this.reportOutputColumnFieldsDeclaration = reportOutputColumnFieldsDeclaration;
            this.reportOutputColumns = reportOutputColumns;
            this.cubeColumnName = parameters.getCubeColumnName();
            this.groupTotalKey = groupTotalKey;
            this.splunkReportColumns = splunkReportColumns;
            this.rootIdsForNonRequiredDimensions = parameters.getRootIdsForNonRequiredDimensions();
            this.columnsForStatsCalculation = parameters.getColumnsForStatsCalculation();
            this.columnIdsForStatsCalculation = parameters.getColumnIdsForStatsCalculation();
            this.dimensionColumnPrepostfix = AccountMasterStatsParameters.DIMENSION_COLUMN_PREPOSTFIX;
            this.finalGroupTotalKey = AccountMasterStatsParameters.GROUP_TOTAL_KEY;

        }
    }
}
