package com.latticeengines.datacloud.dataflow.transformation.stats.report;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.AMStatsFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.AMStatsLeafFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.AccountMasterStatsParameters;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalAttribute;
import com.latticeengines.domain.exposed.datacloud.manage.CategoricalDimension;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

@Component("amStatsLeafNodeFlow")
public class AMStatsLeafNodeFlow extends AMStatsFlowBase {

    @Override
    public Node construct(AccountMasterStatsParameters parameters) {

        Map<String, Map<String, CategoricalAttribute>> requiredDimensionsValuesMap = parameters
                .getRequiredDimensionsValuesMap();

        Node node = addSource(parameters.getBaseTables().get(0));

        Map<String, CategoricalDimension> requiredDimensions = parameters.getRequiredDimensions();

        List<FieldMetadata> schema = node.getSchema();

        Map<String, List<String>> dimensionDefinitionMap = parameters.getDimensionDefinitionMap();

        List<List<FieldMetadata>> leafSchema = //
                getLeafSchema(schema, dimensionDefinitionMap);
        List<FieldMetadata> leafSchemaNewColumns = leafSchema.get(0);
        List<FieldMetadata> leafSchemaOldColumns = leafSchema.get(1);
        List<FieldMetadata> inputSchemaDimensionColumns = leafSchema.get(2);

        List<FieldMetadata> leafSchemaAllOutputColumns = new ArrayList<>();
        leafSchemaAllOutputColumns.addAll(leafSchemaOldColumns);
        leafSchemaAllOutputColumns.addAll(leafSchemaNewColumns);

        FieldList applyToFieldList = getFieldList(inputSchemaDimensionColumns);
        FieldList outputFieldList = getFieldList(leafSchemaAllOutputColumns);

        return createLeafGenerationNode(node, requiredDimensionsValuesMap, //
                requiredDimensions, dimensionDefinitionMap, //
                leafSchemaNewColumns, applyToFieldList, outputFieldList);
    }

    private Node createLeafGenerationNode(Node node,
            Map<String, Map<String, CategoricalAttribute>> requiredDimensionsValuesMap, //
            Map<String, CategoricalDimension> requiredDimensions, //
            Map<String, List<String>> dimensionDefinitionMap, //
            List<FieldMetadata> leafSchemaNewColumns, //
            FieldList applyToFieldList, FieldList outputFieldList) {

        AMStatsLeafFunction.Params functionParams = //
                new AMStatsLeafFunction.Params(getFields(leafSchemaNewColumns), dimensionDefinitionMap, //
                        requiredDimensions, //
                        requiredDimensionsValuesMap);

        AMStatsLeafFunction leafCreationFunction = new AMStatsLeafFunction(functionParams);

        return node.apply(leafCreationFunction, //
                applyToFieldList, leafSchemaNewColumns, outputFieldList);
    }
}
