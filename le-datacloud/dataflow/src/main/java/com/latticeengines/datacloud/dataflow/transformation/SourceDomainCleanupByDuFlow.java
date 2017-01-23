package com.latticeengines.datacloud.dataflow.transformation;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.DomainCleanupByDuBuffer;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.SourceDomainCleanupByDuTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

@Component("sourceDomainCleanupByDuFlow")
public class SourceDomainCleanupByDuFlow extends ConfigurableFlowBase<SourceDomainCleanupByDuTransformerConfig> {
    private static final Log log = LogFactory.getLog(SourceDomainCleanupByDuFlow.class);

    @Override
    public Node construct(TransformationFlowParameters parameters) {

        SourceDomainCleanupByDuTransformerConfig config = getTransformerConfig(parameters);
        Node source = addSource(parameters.getBaseTables().get(0));
        source = source.groupByAndBuffer(
                new FieldList(config.getDuField()),
                new DomainCleanupByDuBuffer(source.getFieldNames(), config.getDuField(), config.getDunsField(), config
                        .getDomainField()));
        return source;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return SourceDomainCleanupByDuTransformerConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return "sourceDomainCleanupByDuFlow";
    }

    @Override
    public String getTransformerName() {
        return "sourceDomainCleanupByDuTransformer";

    }
}