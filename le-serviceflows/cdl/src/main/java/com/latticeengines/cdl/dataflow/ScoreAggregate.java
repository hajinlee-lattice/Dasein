package com.latticeengines.cdl.dataflow;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.Aggregation;
import com.latticeengines.dataflow.exposed.builder.common.AggregationType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.ScoreAggregateParameters;

@Component("scoreAggregate")
public class ScoreAggregate extends TypesafeDataFlowBuilder<ScoreAggregateParameters> {

    @Override
    public Node construct(ScoreAggregateParameters parameters) {
        Node scoreTable = addSource(parameters.getScoreResultsTableName());

        Aggregation aggregation = null;
        String scoreFieldName;
        List<String> fieldNames = scoreTable.getFieldNames();
        if (fieldNames.contains(InterfaceName.ExpectedRevenue.name())) {
            scoreFieldName = InterfaceName.ExpectedRevenue.name();
        } else {
            scoreFieldName = InterfaceName.Probability.name();
        }
        aggregation = new Aggregation(scoreFieldName, "AverageScore", AggregationType.AVG);
        return scoreTable.aggregate(aggregation).limit(1);
    }
}
