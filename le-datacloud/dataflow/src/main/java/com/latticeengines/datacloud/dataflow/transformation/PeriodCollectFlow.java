package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PeriodCollectorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

@Component(PeriodCollectFlow.DATAFLOW_BEAN_NAME)
public class PeriodCollectFlow extends ConfigurableFlowBase<PeriodCollectorConfig> {

    public static final String DATAFLOW_BEAN_NAME = "periodCollectFlow";

    @Override
    public Node construct(TransformationFlowParameters parameters) {

        PeriodCollectorConfig config = getTransformerConfig(parameters);

        Node result = addSource(parameters.getBaseTables().get(0));

        List<String> groupByFields = new ArrayList<>();
        if (StringUtils.isNotBlank(config.getPeriodNameField())) {
            groupByFields.add(config.getPeriodNameField());
        }
        if (StringUtils.isNotBlank(config.getPeriodField())) {
            groupByFields.add(config.getPeriodField());
        }

        result = result.groupByAndLimit(new FieldList(groupByFields), 1);
        result = result.retain(new FieldList(groupByFields));
        return result;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return PeriodCollectorConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return PeriodCollectFlow.DATAFLOW_BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return DataCloudConstants.PERIOD_COLLECTOR;

    }
}
