package com.latticeengines.datacloud.dataflow.transformation;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.Aggregation;
import com.latticeengines.dataflow.exposed.builder.common.AggregationType;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.ConsolidateAddPeriodColumnFunction;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PeriodConvertorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

@Component("periodConvertFlow")
public class PeriodConvertFlow extends ConsolidateBaseFlow<PeriodConvertorConfig> {

    private static final String MIN_COLUMN = "__MIN__";

    @Override
    public Node construct(TransformationFlowParameters parameters) {

        PeriodConvertorConfig config = getTransformerConfig(parameters);

        Node result = addSource(parameters.getBaseTables().get(0));

        result = addPeriodIdColumn(config, result);

        return result;
    }

    private Node addPeriodIdColumn(PeriodConvertorConfig config, Node result) {
        result = result.aggregate(new Aggregation(config.getTrxDateField(), MIN_COLUMN, AggregationType.MIN_STR));
        result = result.apply(
                new ConsolidateAddPeriodColumnFunction(config.getPeriodStrategy(), config.getTrxDateField(), MIN_COLUMN,
                        config.getPeriodField()),
                new FieldList(config.getTrxDateField(), MIN_COLUMN),
                new FieldMetadata(InterfaceName.PeriodId.name(), Integer.class));
        result = result.discard(new FieldList(MIN_COLUMN));
        return result;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return PeriodConvertorConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return "periodConvertFlow";
    }

    @Override
    public String getTransformerName() {
        return DataCloudConstants.PERIOD_CONVERTOR;

    }
}
