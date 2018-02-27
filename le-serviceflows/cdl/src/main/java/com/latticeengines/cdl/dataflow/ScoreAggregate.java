package com.latticeengines.cdl.dataflow;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.Aggregation;
import com.latticeengines.dataflow.exposed.builder.common.AggregationType;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.cdl.AverageScoreAggregator;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.ScoreAggregateParameters;

@Component("scoreAggregate")
public class ScoreAggregate extends TypesafeDataFlowBuilder<ScoreAggregateParameters> {

    private static final String AVG_FIELD = InterfaceName.AverageScore.name();

    private ScoreAggregateParameters params;

    @Override
    public Node construct(ScoreAggregateParameters parameters) {
        this.params = parameters;

        Node scoreTable = addSource(parameters.getScoreResultsTableName());

        if (StringUtils.isNotBlank(parameters.getModelGuidField())) {
            return aggregateMultiModel(scoreTable);
        } else {
            return aggregateSingleModel(scoreTable);
        }

    }

    private Node aggregateMultiModel(Node source) {
        String modelGuidField = params.getModelGuidField();
        Map<String, String> scoreFieldMap = params.getScoreFieldMap();
        AverageScoreAggregator aggregator = new AverageScoreAggregator(modelGuidField, scoreFieldMap, AVG_FIELD);
        List<FieldMetadata> fms = Arrays.asList( //
                new FieldMetadata(modelGuidField, String.class), //
                new FieldMetadata(AVG_FIELD, Double.class));
        return source.groupByAndAggregate(new FieldList(modelGuidField), aggregator, fms);
    }

    private Node aggregateSingleModel(Node source) {
        String scoreFieldName;
        if (params.getExpectedValue()) {
            scoreFieldName = InterfaceName.ExpectedRevenue.name();
        } else {
            scoreFieldName = InterfaceName.Probability.name();
        }
        Aggregation aggregation = new Aggregation(scoreFieldName, AVG_FIELD, AggregationType.AVG);
        return source.aggregate(aggregation).limit(1);
    }
}
