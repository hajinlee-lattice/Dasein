package com.latticeengines.datacloud.dataflow.transformation;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.AccountMasterLookupKeyFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.TransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BasicTransformationConfiguration;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

@Component("accountMasterLookupRebuildFlow")
public class AccountMasterLookupRebuildFlow
        extends TransformationFlowBase<BasicTransformationConfiguration, TransformationFlowParameters> {

    private final static String LATTICEID_FIELD = "LatticeID";
    private final static String KEY_FIELD = "Key";
    private final static String DOMAIN_FIELD = "Domain";
    private final static String DUNS_FIELD = "DUNS";
    private final static String PRIMARY_DOMAIN_FIELD = "LE_IS_PRIMARY_DOMAIN";
    private final static String PRIMARY_LOCATION_FIELD = "LE_IS_PRIMARY_LOCATION";

    @Override
    protected Class<? extends TransformationConfiguration> getTransConfClass() {
        return BasicTransformationConfiguration.class;
    }

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        Node accountMasterSeed = addSource(parameters.getBaseTables().get(0));
        Node searchByDuns = addSearchByDuns(accountMasterSeed);
        Node searchByDomain = addSearchByDomainNode(accountMasterSeed);
        Node searchByBoth = addSearchByBothNode(accountMasterSeed);
        return searchByDuns.merge(searchByDomain).merge(searchByBoth);
    }

    // Accounts from LatticeCacheSeed with domains, no DUNS.
    private Node addSearchByDomainNode(Node node) {
        node = node.filter(DOMAIN_FIELD + " != null", new FieldList(DOMAIN_FIELD));
        node = node.groupByAndLimit(new FieldList(DOMAIN_FIELD), new FieldList(PRIMARY_LOCATION_FIELD), 1, true, true);
        node = node.apply(new AccountMasterLookupKeyFunction(KEY_FIELD, DOMAIN_FIELD, null),
                new FieldList(node.getFieldNames()), new FieldMetadata(KEY_FIELD, String.class));
        return node.retain(new FieldList(LATTICEID_FIELD, KEY_FIELD));
    }

    // Accounts from DnBCacheSeed with DUNS but no Domains
    private Node addSearchByDuns(Node node) {
        node = node.filter(DUNS_FIELD + " != null", new FieldList(DUNS_FIELD));
        node = node.groupByAndLimit(new FieldList(DUNS_FIELD), new FieldList(PRIMARY_DOMAIN_FIELD), 1, true, true);
        node = node.apply(new AccountMasterLookupKeyFunction(KEY_FIELD, null, DUNS_FIELD),
                new FieldList(node.getFieldNames()), new FieldMetadata(KEY_FIELD, String.class));
        return node.retain(new FieldList(LATTICEID_FIELD, KEY_FIELD));
    }

    // Accounts from DnBCacheSeed with both DUNS and Domains
    private Node addSearchByBothNode(Node node) {
        node = node.filter(DOMAIN_FIELD + " != null && " + DUNS_FIELD + " != null",
                new FieldList(DOMAIN_FIELD, DUNS_FIELD));
        node = node.groupByAndLimit(new FieldList(DOMAIN_FIELD, DUNS_FIELD), 1);
        node = node.apply(new AccountMasterLookupKeyFunction(KEY_FIELD, DOMAIN_FIELD, DUNS_FIELD),
                new FieldList(node.getFieldNames()), new FieldMetadata(KEY_FIELD, String.class));
        return node.retain(new FieldList(LATTICEID_FIELD, KEY_FIELD));
    }
}
