package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.Aggregation;
import com.latticeengines.dataflow.exposed.builder.common.AggregationType;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.ConsolidateAddCompositeColumnFuction;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PeriodDataAggregaterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;


@Component("periodDataAggregateFlow")
public class PeriodDataAggregateFlow extends ConsolidateBaseFlow<PeriodDataAggregaterConfig> {

    @Override
    public Node construct(TransformationFlowParameters parameters) {

        PeriodDataAggregaterConfig config = getTransformerConfig(parameters);

        Node result = null;
        for (String sourceName : parameters.getBaseTables()) {
            Node source = addSource(sourceName);
            if (result == null) {
                result = source;
            } else {
                result = result.merge(source);
            }
        }
        result = aggregate(config, result);
        result = result.apply(new ConsolidateAddCompositeColumnFuction(config.getGoupByFields(), COMPOSITE_KEY),
                new FieldList(config.getGoupByFields()), new FieldMetadata(COMPOSITE_KEY, String.class));

        // TODO: a temp way of adding period name
        result = result.addColumnWithFixedValue(InterfaceName.PeriodName.name(), "Month", String.class);

        return result;
    }

    private Node aggregate(PeriodDataAggregaterConfig config, Node result) {
        List<Aggregation> aggregations = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(config.getSumFields())) {
            List<String> sumFields = config.getSumFields();
            List<String> sumOutputFields = config.getSumOutputFields();
            for (int i = 0; i < sumFields.size(); i++) {
                aggregations.add(new Aggregation(sumFields.get(i), sumOutputFields.get(i), AggregationType.SUM));
            }
        }
        if (CollectionUtils.isNotEmpty(config.getSumLongFields())) {
            List<String> sumLongFields = config.getSumLongFields();
            List<String> sumLongOutputFields = config.getSumLongOutputFields();
            for (int i = 0; i < sumLongFields.size(); i++) {
                aggregations.add(new Aggregation(sumLongFields.get(i), sumLongOutputFields.get(i), AggregationType.SUM_LONG));
            }
        }
        if (CollectionUtils.isNotEmpty(config.getCountFields())) {
            List<String> countFields = config.getCountFields();
            List<String> countOutputFields = config.getCountOutputFields();
            for (int i = 0; i < countFields.size(); i++) {
                aggregations.add(new Aggregation(countFields.get(i), countOutputFields.get(i), AggregationType.COUNT));
            }
        }
        result = result.groupBy(new FieldList(config.getGoupByFields()), aggregations);
        return result;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return PeriodDataAggregaterConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return "periodDataAggregateFlow";
    }

    @Override
    public String getTransformerName() {
        return DataCloudConstants.PERIOD_DATA_AGGREGATER;

    }
}
