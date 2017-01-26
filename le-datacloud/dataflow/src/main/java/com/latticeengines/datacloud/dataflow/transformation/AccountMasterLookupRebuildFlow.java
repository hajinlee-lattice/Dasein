package com.latticeengines.datacloud.dataflow.transformation;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.AccountMasterLookupBuffer;
import com.latticeengines.dataflow.runtime.cascading.propdata.AccountMasterLookupKeyFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.TransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BasicTransformationConfiguration;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

@Component("accountMasterLookupRebuildFlow")
public class AccountMasterLookupRebuildFlow
        extends TransformationFlowBase<BasicTransformationConfiguration, TransformationFlowParameters> {

    private final static String LATTICEID_FIELD = "LatticeID";
    private final static String KEY_FIELD = "Key";
    private final static String DOMAIN_FIELD = "Domain";
    private final static String DUNS_FIELD = "DUNS";
    private final static String DOMAIN_MAPPING_PRIMARY_DOMAIN_FIELD = "PrimaryDomain";
    private final static String DOMAIN_MAPPING_SECONDARY_DOMAIN_FIELD = "SecondaryDomain";
    private final static String DOMAIN_MAPPING_PRIMARY_DOMAIN_FIELD_RENAMED = "_" + DOMAIN_MAPPING_PRIMARY_DOMAIN_FIELD
            + "_";
    private final static String PRIMARY_DOMAIN_FIELD = "LE_IS_PRIMARY_DOMAIN";
    private final static String PRIMARY_LOCATION_FIELD = "LE_IS_PRIMARY_LOCATION";
    private final static String GLOBAL_DUNS_FIELD = "GLOBAL_ULTIMATE_DUNS_NUMBER";

    @Override
    protected Class<? extends TransformationConfiguration> getTransConfClass() {
        return BasicTransformationConfiguration.class;
    }

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        Node accountMasterSeed = addSource(parameters.getBaseTables().get(0));

        Node orbCacheSeedSecondaryDomainMapping = addSource(parameters.getBaseTables().get(1));

        orbCacheSeedSecondaryDomainMapping = orbCacheSeedSecondaryDomainMapping.rename(
                new FieldList(DOMAIN_MAPPING_PRIMARY_DOMAIN_FIELD),
                new FieldList(DOMAIN_MAPPING_PRIMARY_DOMAIN_FIELD_RENAMED));

        Node accountMasterSeedWithSecondaryDomain1 = accountMasterSeed.join(new FieldList(DOMAIN_FIELD),
                orbCacheSeedSecondaryDomainMapping, new FieldList(DOMAIN_MAPPING_PRIMARY_DOMAIN_FIELD_RENAMED),
                JoinType.INNER);

        Node accountMasterSeedWithSecondaryDomain = accountMasterSeedWithSecondaryDomain1;

        accountMasterSeedWithSecondaryDomain = accountMasterSeedWithSecondaryDomain.filter(
                DOMAIN_MAPPING_SECONDARY_DOMAIN_FIELD + " != null",
                new FieldList(DOMAIN_MAPPING_SECONDARY_DOMAIN_FIELD));
        accountMasterSeedWithSecondaryDomain = accountMasterSeedWithSecondaryDomain
                .discard(new FieldList(DOMAIN_FIELD, DOMAIN_MAPPING_PRIMARY_DOMAIN_FIELD_RENAMED));
        accountMasterSeedWithSecondaryDomain = accountMasterSeedWithSecondaryDomain
                .rename(new FieldList(DOMAIN_MAPPING_SECONDARY_DOMAIN_FIELD), new FieldList(DOMAIN_FIELD));

        accountMasterSeedWithSecondaryDomain = accountMasterSeedWithSecondaryDomain
                .retain(new FieldList(accountMasterSeed.getFieldNames()));
        accountMasterSeed = accountMasterSeed.merge(accountMasterSeedWithSecondaryDomain);

        Node searchByDuns = addSearchByDuns(accountMasterSeed);

        Node searchByDomain = addSearchByDomainNode(accountMasterSeed);

        Node searchByBoth = addSearchByBothNode(accountMasterSeed);
        return searchByDuns.merge(searchByDomain).merge(searchByBoth);
    }

    private Node addSearchByDomainNode(Node node) {
        node = node.filter(DOMAIN_FIELD + " != null", new FieldList(DOMAIN_FIELD));
        // node = node.groupByAndLimit(new FieldList(DOMAIN_FIELD), new
        // FieldList(PRIMARY_LOCATION_FIELD), 1, true, true);
        node = node.groupByAndBuffer(new FieldList(DOMAIN_FIELD), new FieldList(PRIMARY_LOCATION_FIELD, DUNS_FIELD),
                new AccountMasterLookupBuffer(
                        new Fields(node.getFieldNames().toArray(new String[node.getFieldNames().size()])),
                        LATTICEID_FIELD, DOMAIN_FIELD, DUNS_FIELD, GLOBAL_DUNS_FIELD, PRIMARY_LOCATION_FIELD),
                true);
        node = node.apply(new AccountMasterLookupKeyFunction(KEY_FIELD, DOMAIN_FIELD, null),
                new FieldList(node.getFieldNames()), new FieldMetadata(KEY_FIELD, String.class));
        return node.retain(new FieldList(LATTICEID_FIELD, KEY_FIELD));
    }

    private Node addSearchByDuns(Node node) {
        node = node.filter(DUNS_FIELD + " != null", new FieldList(DUNS_FIELD));
        node = node.groupByAndLimit(new FieldList(DUNS_FIELD), new FieldList(PRIMARY_DOMAIN_FIELD), 1, true, true);
        node = node.apply(new AccountMasterLookupKeyFunction(KEY_FIELD, null, DUNS_FIELD),
                new FieldList(node.getFieldNames()), new FieldMetadata(KEY_FIELD, String.class));
        return node.retain(new FieldList(LATTICEID_FIELD, KEY_FIELD));
    }

    private Node addSearchByBothNode(Node node) {
        node = node.filter(DOMAIN_FIELD + " != null && " + DUNS_FIELD + " != null",
                new FieldList(DOMAIN_FIELD, DUNS_FIELD));
        node = node.groupByAndLimit(new FieldList(DOMAIN_FIELD, DUNS_FIELD), 1);
        node = node.apply(new AccountMasterLookupKeyFunction(KEY_FIELD, DOMAIN_FIELD, DUNS_FIELD),
                new FieldList(node.getFieldNames()), new FieldMetadata(KEY_FIELD, String.class));
        return node.retain(new FieldList(LATTICEID_FIELD, KEY_FIELD));
    }
}
