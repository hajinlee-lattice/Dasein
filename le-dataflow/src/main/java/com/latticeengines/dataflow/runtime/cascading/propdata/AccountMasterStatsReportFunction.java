package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
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
    private static final long serialVersionUID = -2621164226184745299L;

    private int cubeColumnPos;
    private Map<String, Long> rootIdsForNonRequiredDimensions;
    private List<String> leafSchemaNewColumnNames;
    private String groupTotalKey;
    private String dimensionColumnPrepostfix;
    public String lblOrderPost;
    public String lblOrderPreNumeric;
    public String lblOrderPreBoolean;
    public String countKey;

    private ObjectMapper om = new ObjectMapper();

    public AccountMasterStatsReportFunction(Params parameterObject) {
        super(parameterObject.fieldDeclaration);
        leafSchemaNewColumnNames = new ArrayList<String>();
        this.groupTotalKey = parameterObject.groupTotalKey;
        this.rootIdsForNonRequiredDimensions = parameterObject.rootIdsForNonRequiredDimensions;
        this.dimensionColumnPrepostfix = parameterObject.dimensionColumnPrepostfix;
        this.lblOrderPost = parameterObject.lblOrderPost;
        this.lblOrderPreNumeric = parameterObject.lblOrderPreNumeric;
        this.lblOrderPreBoolean = parameterObject.lblOrderPreBoolean;
        this.countKey = parameterObject.countKey;

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

            if (value != null) {
                Map<String, Long> statsMap = JsonUtils.convertMap(JsonUtils.deserialize(value.toString(), Map.class),
                        String.class, Long.class);
                Long valueInt = statsMap.get(countKey);

                AttributeStatistics stats = new AttributeStatistics();
                AttributeStatsDetails rowBasedStatistics = new AttributeStatsDetails();
                rowBasedStatistics.setNonNullCount(valueInt);
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
                        String posPart = key.substring(lblOrderPreNumeric.length(), key.indexOf(lblOrderPost));
                        String lbl = key.substring(key.indexOf(lblOrderPost) + 1);

                        Bucket bucket = new Bucket();
                        bucket.setBucketLabel(lbl);
                        bucket.setCount(statsMap.get(key));
                        bucketArray[Integer.parseInt(posPart)] = bucket;
                    } else if (key.startsWith(lblOrderPreBoolean)) {
                        String posPart = key.substring(lblOrderPreBoolean.length(), key.indexOf(lblOrderPost));
                        String lbl = key.substring(key.indexOf(lblOrderPost) + 1);

                        Bucket bucket = new Bucket();
                        bucket.setBucketLabel(lbl);
                        bucket.setCount(statsMap.get(key));
                        bucketArray[Integer.parseInt(posPart)] = bucket;
                        buckets.setType(BucketType.Boolean);
                    }
                }
                List<Bucket> bucketList = (bucketArray == null)//
                        ? new ArrayList<Bucket>() //
                        : Arrays.asList(bucketArray);

                buckets.setBucketList(bucketList);
                rowBasedStatistics.setBuckets(buckets);
                stats.setRowBasedStatistics(rowBasedStatistics);
                statsList.put(field, stats);
            }
        }

        try

        {
            for (String key : leafSchemaNewColumnNames) {
                String key1 = dimensionColumnPrepostfix + key + dimensionColumnPrepostfix;
                int pos1 = functionCall.getDeclaredFields().getPos(key);
                if (pos1 >= 0) {
                    result.set(pos1, dimensionIds.containsKey(key1) ? dimensionIds.get(key1)
                            : rootIdsForNonRequiredDimensions.get(key));
                }
            }
            result.set(cubeColumnPos, om.writeValueAsString(cube));
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
        public String lblOrderPost;
        public String lblOrderPreNumeric;
        public String lblOrderPreBoolean;
        public String countKey;

        public Params(Fields fieldDeclaration, List<FieldMetadata> leafSchemaNewColumns, String cubeColumnName,
                String groupTotalKey, Map<String, Long> rootIdsForNonRequiredDimensions,
                String dimensionColumnPrepostfix, String lblOrderPost, String lblOrderPreNumeric,
                String lblOrderPreBoolean, String countKey) {
            super();
            this.fieldDeclaration = fieldDeclaration;
            this.leafSchemaNewColumns = leafSchemaNewColumns;
            this.cubeColumnName = cubeColumnName;
            this.groupTotalKey = groupTotalKey;
            this.rootIdsForNonRequiredDimensions = rootIdsForNonRequiredDimensions;
            this.dimensionColumnPrepostfix = dimensionColumnPrepostfix;
            this.lblOrderPost = lblOrderPost;
            this.lblOrderPreNumeric = lblOrderPreNumeric;
            this.lblOrderPreBoolean = lblOrderPreBoolean;
            this.countKey = countKey;
        }
    }
}
