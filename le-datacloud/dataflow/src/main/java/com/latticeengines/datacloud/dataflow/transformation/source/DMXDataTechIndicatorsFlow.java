package com.latticeengines.datacloud.dataflow.transformation.source;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.ConfigurableFlowBase;
import com.latticeengines.datacloud.dataflow.utils.BitEncodeUtils;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.source.TechIndicatorsConfig;

@Component(DMXDataTechIndicatorsFlow.DATAFLOW_BEAN_NAME)
public class DMXDataTechIndicatorsFlow extends ConfigurableFlowBase<TechIndicatorsConfig> {
    public static final String DATAFLOW_BEAN_NAME = "dmxDataTechIndicatorsFlow";
    public static final String TRANSFORMER_NAME = "dmxDataTechIndicatorsTransformer";
    
    private TechIndicatorsConfig config;
    
    @Override
    public String getDataFlowBeanName() {
        return DATAFLOW_BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return TRANSFORMER_NAME;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return TechIndicatorsConfig.class;
    }

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        config = getTransformerConfig(parameters);
        Node dmxDataClean = addSource(parameters.getBaseTables().get(0));
        List<SourceColumn> sourceColumns = parameters.getColumns();
        Node srcColsEncoded = BitEncodeUtils.encode(dmxDataClean, config.getGroupByFields(), sourceColumns);
        return srcColsEncoded;
    }

}
