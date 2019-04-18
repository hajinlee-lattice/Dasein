package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.utils.FileParser;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.BomboraSurgeParseLocFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.BomboraSurgeConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

@Component(BomboraSurgeCleanFlow.DATAFLOW_BEAN_NAME)
public class BomboraSurgeCleanFlow extends ConfigurableFlowBase<BomboraSurgeConfig> {

    public static final String DATAFLOW_BEAN_NAME = "bomboraSurgeCleanFlow";
    public static final String TRANSFORMER_NAME = "bomboraSurgeCleanTransformer";

    private BomboraSurgeConfig config;

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        config = getTransformerConfig(parameters);
        Node node = addSource(parameters.getBaseTables().get(0));
        Map<String, List<NameLocation>> metroLocMap = FileParser.parseBomboraMetroCodes();
        List<FieldMetadata> targetMetadata = new ArrayList<FieldMetadata>();
        targetMetadata.add(new FieldMetadata(config.getCountryField(), String.class));
        targetMetadata.add(new FieldMetadata(config.getStateField(), String.class));
        targetMetadata.add(new FieldMetadata(config.getCityField(), String.class));
        List<String> outputFields = node.getFieldNames();
        outputFields.add(config.getCountryField());
        outputFields.add(config.getStateField());
        outputFields.add(config.getCityField());
        node = node.apply(
                new BomboraSurgeParseLocFunction(
                        new Fields(config.getCountryField(), config.getStateField(), config.getCityField()),
                        metroLocMap, config.getMetroAreaField(), config.getDomainOriginField(),
                        config.getCountryField(), config.getStateField(), config.getCityField()),
                new FieldList(node.getFieldNames()), targetMetadata, new FieldList(outputFields));
        return node;
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
        return BomboraSurgeConfig.class;
    }

}
