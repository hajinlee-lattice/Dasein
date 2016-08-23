package com.latticeengines.propdata.dataflow.refresh;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.AccountMasterLookupKeyFunction;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.propdata.dataflow.SingleBaseSourceRefreshDataFlowParameter;

@Component("accountMasterLookupRefreshFlow")
public class AccountMasterLookupRefreshFlow extends TypesafeDataFlowBuilder<SingleBaseSourceRefreshDataFlowParameter> {

    private final static String LATTICEID_FIELD = "LatticeID";
    private final static String KEY_FIELD = "Key";
    private final static String DOMAIN_FIELD = "Domain";
    private final static String DUNS_FIELD = "DUNS";
    private final static String PRIMARY_DOMAIN_FIELD = "IsPrimaryDomain";
    private final static String PRIMARY_LOCATION_FIELD = "IsPrimaryLocation";

    @Override
    public Node construct(SingleBaseSourceRefreshDataFlowParameter parameters) {
        Node accountMasterSeed = addSource(parameters.getBaseTables().get(0));
        Node onlyDomain = addGetOnlyDomainNode(accountMasterSeed);
        Node onlyDuns = addGetOnlyDunsNode(accountMasterSeed);
        Node both = addGetBothDunsAndDomainNode(accountMasterSeed);
        Node merged = both.merge(onlyDomain).merge(onlyDuns);
        merged = addPopulateKeyNode(merged);
        merged = addRetainNode(merged);
        return merged;
    }

    // Accounts from LatticeCacheSeed.
    private Node addGetOnlyDomainNode(Node node) {
        node = node.filter(DOMAIN_FIELD + " != null && " + DUNS_FIELD + " == null",
                new FieldList(DOMAIN_FIELD, DUNS_FIELD));
        return node.groupByAndLimit(new FieldList(DOMAIN_FIELD), new FieldList(PRIMARY_LOCATION_FIELD), 1, true, true);
    }

    // Accounts from DnBCacheSeed with DUNS but no Domains
    private Node addGetOnlyDunsNode(Node node) {
        node = node.filter(DOMAIN_FIELD + " == null && " + DUNS_FIELD + " != null",
                new FieldList(DOMAIN_FIELD, DUNS_FIELD));
        return node.groupByAndLimit(new FieldList(DUNS_FIELD), new FieldList(PRIMARY_DOMAIN_FIELD), 1, true, true);
    }

    // Accounts from DnBCacheSeed with both DUNS and Domains
    private Node addGetBothDunsAndDomainNode(Node node) {
        node = node.filter(DOMAIN_FIELD + " != null && " + DUNS_FIELD + " != null",
                new FieldList(DOMAIN_FIELD, DUNS_FIELD));
        return node.groupByAndLimit(new FieldList(DOMAIN_FIELD, DUNS_FIELD), 1);
    }

    private Node addPopulateKeyNode(Node node) {
        return node.apply(new AccountMasterLookupKeyFunction(KEY_FIELD, DOMAIN_FIELD, DUNS_FIELD),
                new FieldList(node.getFieldNames()), new FieldMetadata(KEY_FIELD, String.class));
    }

    private Node addRetainNode(Node node) {
        return node.retain(new FieldList(LATTICEID_FIELD, KEY_FIELD));
    }
}
