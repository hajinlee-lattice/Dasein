package com.latticeengines.datacloud.dataflow.transformation;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.AccountMasterSeedMarkerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

@Component("accountMasterSeedCleanupTransformerFlow")
public class AccountMasterSeedCleanupRebuildFlow extends ConfigurableFlowBase<AccountMasterSeedMarkerConfig> {

    private static final String FLAG_DROP_OOB_ENTRY = "_FLAG_DROP_OOB_ENTRY_";
    private static final String FLAG_DROP_SMALL_BUSINESS = "_FLAG_DROP_SMALL_BUSINESS_";
    private static final String FLAG_DROP_INCORRECT_DATA = "_FLAG_DROP_INCORRECT_DATA_";
    private static final String FLAG_DROP_LESS_POPULAR_DOMAIN = "_FLAG_DROP_LESS_POPULAR_DOMAIN_";
    private static final String FLAG_DROP_ORPHAN_ENTRY = "_FLAG_DROP_ORPHAN_ENTRY_";

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        Node node = addSource(parameters.getBaseTables().get(0));

        FieldList fieldList = new FieldList(FLAG_DROP_OOB_ENTRY, FLAG_DROP_SMALL_BUSINESS, FLAG_DROP_INCORRECT_DATA,
                FLAG_DROP_LESS_POPULAR_DOMAIN, FLAG_DROP_ORPHAN_ENTRY);

        node = node.filter(FLAG_DROP_OOB_ENTRY + " == null && " + FLAG_DROP_SMALL_BUSINESS + " == null && "
                + FLAG_DROP_INCORRECT_DATA + " == null && " + FLAG_DROP_ORPHAN_ENTRY + " == null ", fieldList);

        node = node.discard(fieldList);
        return node;
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return AccountMasterSeedMarkerConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return "accountMasterSeedCleanupTransformerFlow";
    }

    @Override
    public String getTransformerName() {
        return "accountMasterSeedCleanupTransformer";

    }
}
