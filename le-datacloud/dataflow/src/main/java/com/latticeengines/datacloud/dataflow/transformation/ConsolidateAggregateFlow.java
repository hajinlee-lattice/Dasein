package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.Aggregation;
import com.latticeengines.dataflow.exposed.builder.common.AggregationType;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateAggregateConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

@Component("consolidateAggregateFlow")
public class ConsolidateAggregateFlow extends ConsolidateBaseFlow<ConsolidateAggregateConfig> {

    @Override
    public Node construct(TransformationFlowParameters parameters) {

        ConsolidateAggregateConfig config = getTransformerConfig(parameters);

        Node result = null;
        List<Aggregation> aggregations = new ArrayList<>();
        aggregations.add(new Aggregation(config.getSumField(), "Total" + config.getSumField(), AggregationType.SUM));
        aggregations.add(new Aggregation(config.getCountField(), "Total" + config.getCountField(),
                AggregationType.COUNT));

        for (String sourceName : parameters.getBaseTables()) {
            Node source = addSource(sourceName);
            String date = sourceName;
            source = source.addColumnWithFixedValue(config.getTrxDateField(), date, String.class);
            source = source.groupBy(new FieldList(config.getGoupByFields()), aggregations);
            if (result == null) {
                result = source;
            } else {
                result = result.merge(source);
            }
        }
        return result;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return ConsolidateAggregateConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return "consolidateAggregateFlow";
    }

    @Override
    public String getTransformerName() {
        return "consolidateAggregateTransformer";

    }
}
