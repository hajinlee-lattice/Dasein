package com.latticeengines.datacloud.dataflow.transformation;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.OrbCacheSeedSecondaryDomainAccumulationTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

@Component("orbCacheSeedSecondaryDomainTransformerFlow")
public class OrbCacheSeedSecondaryDomainAccumulationFlow
        extends ConfigurableFlowBase<OrbCacheSeedSecondaryDomainAccumulationTransformerConfig> {
    private static final Logger log = LoggerFactory.getLogger(OrbCacheSeedSecondaryDomainAccumulationFlow.class);

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        OrbCacheSeedSecondaryDomainAccumulationTransformerConfig config = getTransformerConfig(parameters);

        Node source = addSource(parameters.getBaseTables().get(0));

        List<String> fieldNames = source.getFieldNames();

        for (String fieldName : fieldNames) {
            log.info("Field in input schema " + fieldName);
        }

        String markerFieldName = config.getMarkerFieldName();
        FieldList filterFieldList = new FieldList(markerFieldName);
        String expression = markerFieldName + " == true";
        Node secondaryDomainsNode = source.filter(expression, filterFieldList);
        secondaryDomainsNode = secondaryDomainsNode.retain(new FieldList(config.getDomainMappingFields()));
        secondaryDomainsNode = secondaryDomainsNode.rename(new FieldList(config.getSecondaryDomainFieldName()),
                new FieldList(config.getRenamedSecondaryDomainFieldName()));
        return secondaryDomainsNode;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return OrbCacheSeedSecondaryDomainAccumulationTransformerConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return "orbCacheSeedSecondaryDomainTransformerFlow";
    }

    @Override
    public String getTransformerName() {
        return "orbCacheSeedSecondaryDomainTransformer";

    }
}
