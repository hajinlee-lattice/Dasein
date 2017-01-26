package com.latticeengines.datacloud.dataflow.transformation;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.AccountMasterSeedMarkerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

@Component("accountMasterSeedSecondaryDomainRebuildFlowTransformerFlow")
public class AccountMasterSeedSecondaryDomainRebuildFlow extends ConfigurableFlowBase<AccountMasterSeedMarkerConfig> {

    private static final String DOMAIN = "Domain";
    private static final String FLAG_DROP_LESS_POPULAR_DOMAIN = "_FLAG_DROP_LESS_POPULAR_DOMAIN_";

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        Node node = addSource(parameters.getBaseTables().get(0));

        FieldList fieldList = new FieldList(FLAG_DROP_LESS_POPULAR_DOMAIN);

        node = node.filter(FLAG_DROP_LESS_POPULAR_DOMAIN + " != null", fieldList);

        node = node.retain(new FieldList(DOMAIN, FLAG_DROP_LESS_POPULAR_DOMAIN));
        node = node.rename(new FieldList(DOMAIN, FLAG_DROP_LESS_POPULAR_DOMAIN),
                new FieldList("SecondaryDomain", "PrimaryDomain"));
        return node;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return AccountMasterSeedMarkerConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return "accountMasterSeedSecondaryDomainRebuildFlowTransformerFlow";
    }

    @Override
    public String getTransformerName() {
        return "accountMasterSeedSecondaryDomainRebuildFlowTransformer";

    }
}
