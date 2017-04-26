package com.latticeengines.datacloud.dataflow.transformation.stats.bucket;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.AMStatsFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.AMStatsMinMaxBuffer;
import com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

@Component("amStatsMinMaxFlow")
public class AMStatsMinMaxFlow extends AMStatsFlowBase {

    @Override
    public Node construct(AccountMasterStatsParameters parameters) {

        Node node = addSource(parameters.getBaseTables().get(0));

        List<FieldMetadata> schema = node.getSchema();

        Map<String, List<String>> dimensionDefinitionMap = parameters.getDimensionDefinitionMap();

        List<List<FieldMetadata>> leafSchema = getLeafSchema(schema, dimensionDefinitionMap);
        List<FieldMetadata> leafSchemaNewColumns = leafSchema.get(0);
        List<FieldMetadata> leafSchemaOldColumns = leafSchema.get(1);
        List<FieldMetadata> leafSchemaAllOutputColumns = new ArrayList<>();
        leafSchemaAllOutputColumns.addAll(leafSchemaOldColumns);
        leafSchemaAllOutputColumns.addAll(leafSchemaNewColumns);

        leafSchemaAllOutputColumns.add(new FieldMetadata(getMinMaxKey(), String.class));

        List<FieldMetadata> fms = new ArrayList<>();
        fms.addAll(resultSchema(leafSchemaAllOutputColumns));

        node.renamePipe("leafRecordsNode");
        node = createGroupingAndMinMaxAssessNode(node, leafSchemaAllOutputColumns);

        return node;
    }

    private Node createGroupingAndMinMaxAssessNode(Node node, List<FieldMetadata> finalLeafSchema) {
        Fields minMaxResultFields = new Fields();

        List<FieldMetadata> fms = new ArrayList<>();

        for (FieldMetadata fieldMeta : finalLeafSchema) {
            if (fieldMeta.getFieldName().equals(getMinMaxKey())) {
                minMaxResultFields = minMaxResultFields
                        .append(new Fields(fieldMeta.getFieldName(), fieldMeta.getJavaType()));
                fms.add(fieldMeta);
            }
        }

        minMaxResultFields = minMaxResultFields.append(new Fields(MIN_MAX_JOIN_FIELD, Integer.class));

        AMStatsMinMaxBuffer.Params functionParams = //
                new AMStatsMinMaxBuffer.Params(minMaxResultFields, getMinMaxKey());

        AMStatsMinMaxBuffer buffer = new AMStatsMinMaxBuffer(functionParams);

        node = node.addColumnWithFixedValue(MIN_MAX_JOIN_FIELD, 0, Integer.class);

        fms.add(new FieldMetadata(MIN_MAX_JOIN_FIELD, Integer.class));

        Node minMaxNode = node.groupByAndBuffer(new FieldList(MIN_MAX_JOIN_FIELD), buffer, fms);

        return minMaxNode;
    }

    private List<FieldMetadata> resultSchema(List<FieldMetadata> leafSchemaAllOutputColumns) {
        List<FieldMetadata> resultSchema = new ArrayList<>();

        for (FieldMetadata metadata : leafSchemaAllOutputColumns) {
            resultSchema.add(new FieldMetadata(metadata.getFieldName(), String.class));
        }

        return resultSchema;
    }

}
