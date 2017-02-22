package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import cascading.tuple.Fields;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.DomainCleanupByDuBuffer;
import com.latticeengines.dataflow.runtime.cascading.propdata.FillBlankDomainBuffer;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.SourceDomainCleanupByDuTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

@Component("sourceDomainCleanupByDuFlow")
public class SourceDomainCleanupByDuFlow extends ConfigurableFlowBase<SourceDomainCleanupByDuTransformerConfig> {
    private static final Log log = LogFactory.getLog(SourceDomainCleanupByDuFlow.class);

    @Override
    public Node construct(TransformationFlowParameters parameters) {

        SourceDomainCleanupByDuTransformerConfig config = getTransformerConfig(parameters);
        Node source = addSource(parameters.getBaseTables().get(0));

        Fields duAndDomain = new Fields(new String[] { config.getDuField(), DomainCleanupByDuBuffer.DU_PRIMARY_DOMAIN });
        List<FieldMetadata> fms = new ArrayList<>();
        fms.add(new FieldMetadata(config.getDuField(), String.class));
        fms.add(new FieldMetadata(DomainCleanupByDuBuffer.DU_PRIMARY_DOMAIN, String.class));
        Node duDomain = source.groupByAndBuffer(
                new FieldList(config.getDuField()),
                new DomainCleanupByDuBuffer(duAndDomain, config.getDuField(), config.getDunsField(), config
                        .getDomainField(), config.getAlexaRankField()), fms);
        duDomain = duDomain.renamePipe("dudomain");

        Node join = source.leftOuterJoin(config.getDuField(), duDomain, config.getDuField());
        Fields joinNodeFields = new Fields(join.getFieldNames().toArray(new String[join.getFieldNames().size()]));
        Node domainFilled = join.groupByAndBuffer(new FieldList(config.getDuField()), new FillBlankDomainBuffer(
                joinNodeFields, config.getDomainField()));
        domainFilled = domainFilled.retain(new FieldList(source.getFieldNames()));

        return domainFilled;
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