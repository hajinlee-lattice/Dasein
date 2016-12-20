package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.latticeengines.common.exposed.util.AMStatsUtils;
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

@SuppressWarnings("rawtypes")
public class AccountMasterStatsReportFunction extends BaseOperation implements Function {
    private int cubeColumnPos;
    private Map<String, Long> rootIdsForNonRequiredDimensions;
    private List<String> leafSchemaNewColumnNames;
    private String groupTotalKey;
    private String dimensionColumnPrepostfix;

    public AccountMasterStatsReportFunction(Params parameterObject) {
        super(parameterObject.fieldDeclaration);
        leafSchemaNewColumnNames = new ArrayList<String>();
        this.groupTotalKey = parameterObject.groupTotalKey;
        this.rootIdsForNonRequiredDimensions = parameterObject.rootIdsForNonRequiredDimensions;
        this.dimensionColumnPrepostfix = parameterObject.dimensionColumnPrepostfix;

        int idx = 0;
        for (FieldMetadata metadata : parameterObject.leafSchemaNewColumns) {
            leafSchemaNewColumnNames.add(metadata.getFieldName());
            if (metadata.getFieldName().equals(parameterObject.cubeColumnName)) {
                cubeColumnPos = idx;
            }
            idx++;
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

        while (itr.hasNext()) {

            String field = (String) itr.next();
            Object value = tuple.getObject(pos++);

            if (field.startsWith(dimensionColumnPrepostfix)) {

                if (field.equals(groupTotalKey)) {
                    cube.setNonNullCount((Long) value);
                } else {
                    dimensionIds.put(field, value);
                }
                continue;
            }

            if (value != null && value instanceof String) {
                try {
                    Integer valueInt = Integer.parseInt((String) value);
                    AttributeStatistics stats = new AttributeStatistics();
                    AttributeStatsDetails rowBasedStatistics = new AttributeStatsDetails();
                    rowBasedStatistics.setNonNullCount(valueInt);
                    Buckets buckets = new Buckets();
                    buckets.setType(BucketType.Numerical);
                    List<Bucket> bucketList = new ArrayList<>();
                    Bucket bucket = new Bucket();
                    bucket.setBucketLabel("All");
                    bucket.setCount(valueInt);
                    bucketList.add(bucket);
                    buckets.setBucketList(bucketList);
                    rowBasedStatistics.setBuckets(buckets);
                    stats.setRowBasedStatistics(rowBasedStatistics);
                    statsList.put(field, stats);
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                }
            }
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
            functionCall.getOutputCollector().add(result);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static class Params {
        public Fields fieldDeclaration;
        public List<FieldMetadata> leafSchemaNewColumns;
        public String cubeColumnName;
        public String groupTotalKey;
        public Map<String, Long> rootIdsForNonRequiredDimensions;
        public String dimensionColumnPrepostfix;

        public Params(Fields fieldDeclaration, List<FieldMetadata> leafSchemaNewColumns, String cubeColumnName,
                String groupTotalKey, Map<String, Long> rootIdsForNonRequiredDimensions,
                String dimensionColumnPrepostfix) {
            this.fieldDeclaration = fieldDeclaration;
            this.leafSchemaNewColumns = leafSchemaNewColumns;
            this.cubeColumnName = cubeColumnName;
            this.groupTotalKey = groupTotalKey;
            this.rootIdsForNonRequiredDimensions = rootIdsForNonRequiredDimensions;
            this.dimensionColumnPrepostfix = dimensionColumnPrepostfix;
        }
    }
}
