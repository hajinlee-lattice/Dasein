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

/**
 * A pipeline step of BuiltWithTechIndicator pipeline
 * https://confluence.lattice-engines.com/display/ENG/AccountMaster+Rebuild+Pipelines#AccountMasterRebuildPipelines-BuiltWithTechIndicatorsCreation
 */
@Component(BuiltWithTechIndicatorsFlow.DATAFLOW_BEAN_NAME)
public class BuiltWithTechIndicatorsFlow extends ConfigurableFlowBase<TechIndicatorsConfig> {

    public static final String DATAFLOW_BEAN_NAME = "builtWithTechIndicatorsFlow";

    public static final String TRANSFORMER_NAME = "builtWithTechIndicatorsTransformer";

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        TechIndicatorsConfig config = getTransformerConfig(parameters);
        Node source = addSource(parameters.getBaseTables().get(0));
        List<SourceColumn> sourceColumns = parameters.getColumns();
        Node encoded = BitEncodeUtils.encode(source, config.getGroupByFields(), sourceColumns);
        return encoded.addTimestamp(config.getTimestampField());
    }

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

}
