package com.latticeengines.datacloud.dataflow.transformation;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PeriodCollectorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

@Component("periodCollectFlow")
public class PeriodCollectFlow extends ConfigurableFlowBase<PeriodCollectorConfig> {

    @Override
    public Node construct(TransformationFlowParameters parameters) {

        PeriodCollectorConfig config = getTransformerConfig(parameters);

        String periodField = config.getPeriodField();
        Node result = addSource(parameters.getBaseTables().get(0));

        FieldList fieldList = new FieldList(periodField);
        result = result.groupByAndLimit(fieldList, 1);
        result = result.retain(periodField);
        return result;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return PeriodCollectorConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return "periodCollectFlow";
    }

    @Override
    public String getTransformerName() {
        return DataCloudConstants.PERIOD_COLLECTOR;

    }
}
