package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.ConsolidateAddDateColumnFuction;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PeriodDateConvertorConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

@Component(PeriodDateConvertFlow.DATAFLOW_BEAN_NAME)
public class PeriodDateConvertFlow extends ConfigurableFlowBase<PeriodDateConvertorConfig> {

    public static final String DATAFLOW_BEAN_NAME = "periodDateConvertFlow";

    @Override
    public Node construct(TransformationFlowParameters parameters) {

        PeriodDateConvertorConfig config = getTransformerConfig(parameters);

        Node result = addSource(parameters.getBaseTables().get(0));

        List<FieldMetadata> fms = new ArrayList<FieldMetadata>();
        fms.add(new FieldMetadata(config.getTrxDateField(), String.class));
        fms.add(new FieldMetadata(config.getTrxDayPeriodField(), Integer.class));

        List<String> fieldNames = result.getFieldNames();
        fieldNames.add(config.getTrxDateField());
        fieldNames.add(config.getTrxDayPeriodField());

        result = result.apply(new ConsolidateAddDateColumnFuction(config.getTrxTimeField(), config.getTrxDateField(), config.getTrxDayPeriodField()),
                     new FieldList(config.getTrxTimeField()), fms, new FieldList(fieldNames));
        return result;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return PeriodDateConvertorConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return PeriodDateConvertFlow.DATAFLOW_BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return DataCloudConstants.PERIOD_DATE_CONVERTOR;

    }
}
