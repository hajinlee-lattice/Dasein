package com.latticeengines.datacloud.dataflow.transformation.seed;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.ConfigurableFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.seed.OrbCacheSeedMarkerFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.seed.OrbCacheSeedMarkerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.operation.Function;

/**
 * A pipeline step in OrbCacheSeed rebuild pipeline
 * https://confluence.lattice-engines.com/display/ENG/AccountMaster+Rebuild+Pipelines#AccountMasterRebuildPipelines-OrbCacheSeedCreation
 */
@Component(OrbCacheSeedMarkerFlow.DATAFLOW_BEAN)
public class OrbCacheSeedMarkerFlow extends ConfigurableFlowBase<OrbCacheSeedMarkerConfig> {

    public static final String DATAFLOW_BEAN = "orbCacheSeedMarkerTransformerFlow";
    public static final String TRANSFORMER = "orbCacheSeedMarkerTransformer";

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        OrbCacheSeedMarkerConfig config = getTransformerConfig(parameters);

        Node source = addSource(parameters.getBaseTables().get(0));

        List<String> fieldNames = source.getFieldNames();

        List<String> outputFieldNames = new ArrayList<>(fieldNames);
        Function<?> function = new OrbCacheSeedMarkerFunction(config.getMarkerFieldName(), config.getFieldsToCheck());
        FieldList fieldsToApply = new FieldList(config.getFieldsToCheck());
        List<FieldMetadata> targetFields = new ArrayList<>();
        FieldMetadata secondaryDomainMarker = new FieldMetadata(config.getMarkerFieldName(), Boolean.class);
        targetFields.add(secondaryDomainMarker);

        outputFieldNames.add(config.getMarkerFieldName());
        FieldList outputFields = new FieldList(outputFieldNames);
        Node marked = source.apply(function, fieldsToApply, targetFields, outputFields);
        return marked;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return OrbCacheSeedMarkerConfig.class;
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
