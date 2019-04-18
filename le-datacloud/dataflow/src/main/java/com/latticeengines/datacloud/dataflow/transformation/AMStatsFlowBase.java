package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.TransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.BasicTransformationConfiguration;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;

import cascading.tuple.Fields;

public abstract class AMStatsFlowBase
        extends TransformationFlowBase<BasicTransformationConfiguration, AccountMasterStatsParameters> {

    protected static final String MIN_MAX_JOIN_FIELD = "_JoinFieldMinMax_";

    @Autowired
    protected ColumnMetadataProxy columnMetadataProxy;

    @Override
    public Class<? extends TransformationConfiguration> getTransConfClass() {
        return BasicTransformationConfiguration.class;
    }

    protected FieldList getFieldList(List<FieldMetadata> fieldMetadataList) {
        List<String> fields = new ArrayList<>();
        for (FieldMetadata field : fieldMetadataList) {
            fields.add(field.getFieldName());
        }
        FieldList fieldList = new FieldList(fields);
        return fieldList;
    }

    protected Fields getFields(List<FieldMetadata> fieldMetadataList) {
        String[] fieldArr = new String[fieldMetadataList.size()];
        int idx = 0;
        for (FieldMetadata field : fieldMetadataList) {
            fieldArr[idx++] = field.getFieldName();
        }
        return new Fields(fieldArr);
    }

    protected String getTotalKey() {
        return AccountMasterStatsParameters.GROUP_TOTAL_KEY_TEMP;
    }

    protected String getMinMaxKey() {
        return AccountMasterStatsParameters.MIN_MAX_KEY;
    }

    protected List<List<FieldMetadata>> getLeafSchema(List<FieldMetadata> schema,
            Map<String, List<String>> dimensionDefinitionMap) {
        List<FieldMetadata> leafSchemaOldColumns = new ArrayList<>();
        List<FieldMetadata> leafSchemaNewColumns = new ArrayList<>();
        List<FieldMetadata> inputSchemaDimensionColumns = new ArrayList<>();
        Map<String, String> subDimensionMap = new HashMap<>();

        for (String key : dimensionDefinitionMap.keySet()) {
            List<String> dimensionSubList = dimensionDefinitionMap.get(key);
            for (String subDimension : dimensionSubList) {
                subDimensionMap.put(subDimension, key);
            }
        }

        Set<String> tempTrackingSet = new HashSet<>();

        for (FieldMetadata field : schema) {
            String fieldName = field.getFieldName();
            if (subDimensionMap.containsKey(fieldName)) {
                inputSchemaDimensionColumns.add(field);

                String dimensionId = subDimensionMap.get(fieldName);

                if (tempTrackingSet.contains(dimensionId)) {
                    continue;
                }

                tempTrackingSet.add(dimensionId);
                FieldMetadata dimensionIdSchema = new FieldMetadata(//
                        Schema.Type.LONG, Long.class, field.getFieldName(), //
                        field.getField(), field.getProperties(), null);
                dimensionIdSchema.setFieldName(dimensionId);
                leafSchemaNewColumns.add(dimensionIdSchema);
                continue;
            }
            leafSchemaOldColumns.add(field);
        }

        List<List<FieldMetadata>> leafSchema = new ArrayList<>();
        leafSchema.add(leafSchemaNewColumns);
        leafSchema.add(leafSchemaOldColumns);
        leafSchema.add(inputSchemaDimensionColumns);
        return leafSchema;
    }
}
