package com.latticeengines.datacloud.dataflow.transformation;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.AMSeedSecondDomainCleanupConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

@Component(AMSeedSecondDomainCleanup.DATAFLOW_BEAN_NAME)
public class AMSeedSecondDomainCleanup extends ConfigurableFlowBase<AMSeedSecondDomainCleanupConfig> {

    private AMSeedSecondDomainCleanupConfig config;

    public static final String DATAFLOW_BEAN_NAME = "AMSeedSecondDomainCleanup";
    public static final String TRANSFORMER_NAME = "AMSeedSecondDomainCleanup";

    private static final String SECOND_DOMAIN = "OrbSecondDomain";

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        config = getTransformerConfig(parameters);

        String amDomain = config.getDomainField();
        String amDuns = config.getDunsField();
        String orbSecDomain = config.getSecondDomainField();

        Node amSeed = addSource(parameters.getBaseTables().get(0));
        Node orbSeed = addSource(parameters.getBaseTables().get(1));

        FieldList orbSecDomainField = new FieldList(orbSecDomain);
        FieldList secDomainField = new FieldList(SECOND_DOMAIN);

        // Construct a list of second domains
        Node secondDomain = orbSeed.filter(orbSecDomain + " != null", orbSecDomainField) //
                .groupByAndLimit(orbSecDomainField, 1) //
                .rename(orbSecDomainField, secDomainField) //
                .retain(secDomainField) //
                .renamePipe("secondDomain");

        // Get rid of domain-only records which matches a secondary domain
        amSeed = amSeed.join(new FieldList(amDomain), secondDomain, secDomainField, JoinType.LEFT) //
                .filter("(" + SECOND_DOMAIN + " == null) || (" + amDuns + " != null)", new FieldList(SECOND_DOMAIN, amDuns)) //
                .discard(secDomainField);

        return amSeed;
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
        return AMSeedSecondDomainCleanupConfig.class;
    }
}
