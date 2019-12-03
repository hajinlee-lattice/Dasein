package com.latticeengines.datacloud.dataflow.transformation.seed;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.ConfigurableFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.OrbCacheSeedSecDomainRebuildConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

/**
 * A pipeline step in OrbCacheSeed rebuild pipeline
 * https://confluence.lattice-engines.com/display/ENG/AccountMaster+Rebuild+Pipelines#AccountMasterRebuildPipelines-OrbCacheSeedCreation
 */
@Component(OrbCacheSeedSecDomainFlow.DATAFLOW_BEAN)
public class OrbCacheSeedSecDomainFlow extends ConfigurableFlowBase<OrbCacheSeedSecDomainRebuildConfig> {

    public static final String DATAFLOW_BEAN = "orbCacheSeedSecondaryDomainTransformerFlow";
    public static final String TRANSFORMER = "orbCacheSeedSecondaryDomainTransformer";

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        OrbCacheSeedSecDomainRebuildConfig config = getTransformerConfig(parameters);

        Node source = addSource(parameters.getBaseTables().get(0));
        String markerFieldName = config.getMarkerFieldName();
        FieldList filterFieldList = new FieldList(markerFieldName);
        String expression = markerFieldName + " == true";
        Node secDomainNode = source.filter(expression, filterFieldList)//
                .retain(new FieldList(config.getDomainMappingFields())) //
                .rename(new FieldList(config.getSecondaryDomainFieldName()),
                        new FieldList(config.getRenamedSecondaryDomainFieldName()));
        return secDomainNode;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return OrbCacheSeedSecDomainRebuildConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return DATAFLOW_BEAN;
    }

    @Override
    public String getTransformerName() {
        return TRANSFORMER;

    }
}
