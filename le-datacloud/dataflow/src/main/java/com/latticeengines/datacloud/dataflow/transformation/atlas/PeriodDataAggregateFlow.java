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
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PeriodDataAggregaterConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;


@Component(PeriodDataAggregateFlow.DATAFLOW_BEAN_NAME)
public class PeriodDataAggregateFlow extends ConsolidateBaseFlow<PeriodDataAggregaterConfig> {

    public static final String DATAFLOW_BEAN_NAME = "periodDataAggregateFlow";

    @Override
    public Node construct(TransformationFlowParameters parameters) {

        PeriodDataAggregaterConfig config = getTransformerConfig(parameters);

        Node result = null;
        for (String sourceName : parameters.getBaseTables()) {
            Node source = addSource(sourceName);
            if (config.getSumFields().contains(InterfaceName.Cost.name())
                    && source.getSchema(InterfaceName.Cost.name()) == null) {
                source = source.addColumnWithFixedValue(InterfaceName.Cost.name(), null, Long.class);
            }
            if (result == null) {
                result = source;
            } else {
                result = result.merge(source);
            }
        }
        result = aggregate(config, result);
        result = result.apply(new ConsolidateAddCompositeColumnFuction(config.getGroupByFields(), COMPOSITE_KEY),
                new FieldList(config.getGroupByFields()), new FieldMetadata(COMPOSITE_KEY, String.class));

        if (!result.getFieldNames().contains(InterfaceName.PeriodName.name())) {
            result = result.addColumnWithFixedValue(InterfaceName.PeriodName.name(), null, String.class);
        }

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
        result = result.groupBy(new FieldList(config.getGroupByFields()), aggregations);
        return result;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return PeriodDataAggregaterConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return PeriodDataAggregateFlow.DATAFLOW_BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return DataCloudConstants.PERIOD_DATA_AGGREGATER;

    }
}
