package com.latticeengines.scoring.dataflow;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.springframework.stereotype.Component;

import cascading.tuple.Fields;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.cdl.CalculateExpectedRevenueFunction;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.RecalculateExpectedRevenueParameters;

@Component("recalculateExpectedRevenue")
public class RecalculateExpectedRevenue extends TypesafeDataFlowBuilder<RecalculateExpectedRevenueParameters> {
    @Override
    public Node construct(RecalculateExpectedRevenueParameters parameters) {

        Node inputTable = addSource(parameters.getInputTableName());
        FieldList originalFields = new FieldList(inputTable.getFieldNames());

        String modelGuidFieldName = parameters.getModelGuidField();
        String expectedRevenueFieldName = parameters.getExpectedRevenueFieldName();
        String percentileFieldName = parameters.getPercentileFieldName();
        String predictedRevenuePercentileFieldName = parameters.getPredictedRevenuePercentileFieldName();
        Map<String, String> originalScoreFieldMap = parameters.getOriginalScoreFieldMap();
        Map<String, String> fitFunctionParametersMap = parameters.getFitFunctionParametersMap();

        if (MapUtils.isNotEmpty(fitFunctionParametersMap)) {
            return calculateExpectedRevenueByFieldMap(originalScoreFieldMap, fitFunctionParametersMap,
                                                      modelGuidFieldName,
                                                      percentileFieldName,
                                                      predictedRevenuePercentileFieldName,
                                                      expectedRevenueFieldName,
                                                      inputTable).retain(originalFields);
        } else {
            return inputTable;
        }
    }

    private Node calculateExpectedRevenueByFieldMap(Map<String, String> originalScoreFieldMap, //
                                                    Map<String, String> fitFunctionParametersMap,
                                                    String modelGuidFieldName,
                                                    String percentileFieldName,
                                                    String predictedRevenuePercentileFieldName,
                                                    String expectedRevenueFieldName,
                                                    Node input) {

        Map<String, Node> nodes = splitNodes(input, originalScoreFieldMap, modelGuidFieldName);
        Node merged = null;
        for (Map.Entry<String, Node> entry : nodes.entrySet()) {
            String modelGuid = entry.getKey();
            Node node = entry.getValue();
            String evFitFunctionParameterStr = fitFunctionParametersMap.get(modelGuid);
            Node output = (evFitFunctionParameterStr == null) ? node :
                calculateExpectedRevenue(percentileFieldName, predictedRevenuePercentileFieldName,
                                         expectedRevenueFieldName, evFitFunctionParameterStr, node);
            if (merged == null) {
                merged = output;
            } else {
                merged = merged.merge(output);
            }
        }
        return merged;
    }

    private Map<String, Node> splitNodes(Node input, Map<String, String> originalScoreFieldMap, String modelGuidFieldName) {
        Map<String, Node> nodes = new HashMap<>();
        originalScoreFieldMap.forEach((modelGuid, scoreField) -> {
            Node model = input.filter(String.format("\"%s\".equals(%s)", modelGuid, modelGuidFieldName),
                                      new FieldList(ScoreResultField.ModelId.displayName));
            model = model.renamePipe(modelGuid);
            nodes.put(modelGuid, model);
        });
        return nodes;
    }

    private Node calculateExpectedRevenue(String percentileFieldName,
                                          String predictedRevenuePercentileFieldName,
                                          String expectedRevenueFieldName,
                                          String evFitFunctionParameterStr, Node node) {

        List<String> returnedFields = new ArrayList<>(node.getFieldNames());
        List<FieldMetadata> returnedMetadata = new ArrayList<>(node.getSchema());
        Node calculatePercentile = node.applyToAllFields(
            new CalculateExpectedRevenueFunction(new Fields(returnedFields.toArray(new String[0])),
                                                 percentileFieldName,
                                                 predictedRevenuePercentileFieldName,
                                                 expectedRevenueFieldName,
                                                 evFitFunctionParameterStr),
            returnedMetadata,
            FieldList.RESULTS);

        return calculatePercentile;
    }
}
