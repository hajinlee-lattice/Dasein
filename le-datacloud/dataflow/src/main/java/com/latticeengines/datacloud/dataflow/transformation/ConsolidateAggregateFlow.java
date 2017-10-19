package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.Aggregation;
import com.latticeengines.dataflow.exposed.builder.common.AggregationType;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.ConsolidateAddCompositeColumnFuction;
import com.latticeengines.dataflow.runtime.cascading.propdata.ConsolidateAddDateColumnFuction;
import com.latticeengines.dataflow.runtime.cascading.propdata.ConsolidateAddPeriodColumnFuction;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.ConsolidateAggregateConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

@Component("consolidateAggregateFlow")
public class ConsolidateAggregateFlow extends ConsolidateBaseFlow<ConsolidateAggregateConfig> {

    private static final String MIN_COLUMN = "__MIN__";

    @Override
    public Node construct(TransformationFlowParameters parameters) {

        ConsolidateAggregateConfig config = getTransformerConfig(parameters);

        Node result = null;
        for (String sourceName : parameters.getBaseTables()) {
            Node source = addSource(sourceName);
            if (result == null) {
                result = source;
            } else {
                result = result.merge(source);
            }
        }
        result = result.apply(new ConsolidateAddDateColumnFuction(config.getTrxTimeField(), config.getTrxDateField()),
                new FieldList(config.getTrxTimeField()), new FieldMetadata(config.getTrxDateField(), String.class));
        result = aggregate(config, result);
        result = addPeriodIdColumn(config, result);
        result = result.apply(new ConsolidateAddCompositeColumnFuction(config.getGoupByFields(), COMPOSITE_KEY),
                new FieldList(config.getGoupByFields()), new FieldMetadata(COMPOSITE_KEY, String.class));
        return result;
    }

    private Node aggregate(ConsolidateAggregateConfig config, Node result) {
        List<Aggregation> aggregations = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(config.getSumFields())) {
            for (String field : config.getSumFields()) {
                aggregations.add(new Aggregation(field, "Total" + field, AggregationType.SUM));
            }
        }
        if (CollectionUtils.isNotEmpty(config.getSumLongFields())) {
            for (String field : config.getSumLongFields()) {
                aggregations.add(new Aggregation(field, "Total" + field, AggregationType.SUM_LONG));
            }
        }
        if (CollectionUtils.isNotEmpty(config.getCountFields())) {
            for (String field : config.getCountFields()) {
                aggregations.add(new Aggregation(field, "Total" + field, AggregationType.COUNT));
            }
        }
        result = result.groupBy(new FieldList(config.getGoupByFields()), aggregations);
        return result;
    }

    private Node addPeriodIdColumn(ConsolidateAggregateConfig config, Node result) {
        result = result.aggregate(new Aggregation(config.getTrxDateField(), MIN_COLUMN, AggregationType.MIN_STR));
        result = result.apply(
                new ConsolidateAddPeriodColumnFuction(config.getPeriodStrategy(), config.getTrxDateField(), MIN_COLUMN,
                        InterfaceName.PeriodId.name()),
                new FieldList(config.getTrxDateField(), MIN_COLUMN),
                new FieldMetadata(InterfaceName.PeriodId.name(), Integer.class));
        result = result.discard(new FieldList(MIN_COLUMN));
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
