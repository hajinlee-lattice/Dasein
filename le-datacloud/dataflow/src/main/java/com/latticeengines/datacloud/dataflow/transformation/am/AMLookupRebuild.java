package com.latticeengines.datacloud.dataflow.transformation.am;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ATTR_COUNTRY;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ATTR_DU_DUNS;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ATTR_GU_DUNS;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ATTR_IS_PRIMARY_DOMAIN;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ATTR_IS_PRIMARY_LOCATION;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ATTR_LDC_DOMAIN;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ATTR_LDC_DUNS;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ATTR_STATE;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ATTR_ZIPCODE;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.LATTIC_ID;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ORBSEC_ATTR_PRIDOM;
import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ORBSEC_ATTR_SECDOM;

import java.util.Arrays;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.ConfigurableFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.AMLookupKeyFunction;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

@Component(AMLookupRebuild.DATAFLOW_BEAN_NAME)
public class AMLookupRebuild extends ConfigurableFlowBase<TransformerConfig> {

    public static final String DATAFLOW_BEAN_NAME = "AMLookupRebuildFlow";
    public static final String TRANSFORMER_NAME = "AMLookupRebuild";

    public static final String KEY = "Key";
    private static final String[] AMLOOKUP_FINAL_FIELDS = { LATTIC_ID, KEY, ATTR_LDC_DUNS, ATTR_DU_DUNS, ATTR_GU_DUNS };

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        Node amSeed = addSource(parameters.getBaseTables().get(0));
        Node orbSeed = addSource(parameters.getBaseTables().get(1));

        amSeed = appendSecondaryDomain(amSeed, orbSeed);

        Node searchByDuns = searchByDuns(amSeed.renamePipe("duns"));
        Node searchByDomain = searchByDomain(amSeed.renamePipe("domain"));
        Node searchByDomainCountryZipCode = searchByDomainCountryZipCode(amSeed.renamePipe("domaincountryzip"));
        Node searchByDomainCountryState = searchByDomainCountryState(amSeed.renamePipe("domaincountrystate"));
        Node searchByDomainCountry = searchByDomainCountry(amSeed.renamePipe("domaincountry"));
        Node searchByBoth = searchByBoth(amSeed.renamePipe("both"));

        return searchByDuns.merge(Arrays.asList( //
                searchByDomain, //
                searchByDomainCountryZipCode, //
                searchByDomainCountryState, //
                searchByDomainCountry, //
                searchByBoth));
    }

    private Node appendSecondaryDomain(Node amSeed, Node orbSeed) {
        orbSeed = orbSeed.filter(ORBSEC_ATTR_PRIDOM + " != null", new FieldList(ORBSEC_ATTR_PRIDOM)) //
                .retain(new FieldList(ORBSEC_ATTR_PRIDOM, ORBSEC_ATTR_SECDOM));

        // join find sec domain for domain == pri domain
        Node hasSd = amSeed.join(new FieldList(ATTR_LDC_DOMAIN), orbSeed, new FieldList(ORBSEC_ATTR_PRIDOM),
                JoinType.INNER);
        hasSd = hasSd.filter(ORBSEC_ATTR_SECDOM + " != null", new FieldList(ORBSEC_ATTR_SECDOM)) //
                .discard(new FieldList(ATTR_LDC_DOMAIN));

        Node domains = amSeed.retain(new FieldList(ATTR_LDC_DOMAIN)) //
                .groupByAndLimit(new FieldList(ATTR_LDC_DOMAIN), 1);

        hasSd = hasSd.renamePipe("hasSD") //
                .leftJoin(new FieldList(ORBSEC_ATTR_SECDOM), domains, new FieldList(ATTR_LDC_DOMAIN));

        // secondary not exist in am seed
        Node toAppend = hasSd
                .filter(ORBSEC_ATTR_SECDOM + " != null && " + ATTR_LDC_DOMAIN + " == null",
                        new FieldList(ORBSEC_ATTR_SECDOM, ATTR_LDC_DOMAIN)) //
                .discard(new FieldList(ATTR_LDC_DOMAIN, ORBSEC_ATTR_PRIDOM)) //
                .rename(new FieldList(ORBSEC_ATTR_SECDOM), new FieldList(ATTR_LDC_DOMAIN)) //
                .retain(new FieldList(amSeed.getFieldNames()));

        return amSeed.merge(toAppend);
    }

    private Node searchByDomain(Node node) {
        node = node.filter(ATTR_LDC_DOMAIN + " != null", new FieldList(ATTR_LDC_DOMAIN));
        node = node.groupByAndLimit(new FieldList(ATTR_LDC_DOMAIN), new FieldList(ATTR_IS_PRIMARY_LOCATION), 1, true,
                true);
        node = node.apply(
                new AMLookupKeyFunction(KEY, ATTR_LDC_DOMAIN, null, null, null, null),
                new FieldList(node.getFieldNames()), new FieldMetadata(KEY, String.class));
        return node.retain(new FieldList(AMLOOKUP_FINAL_FIELDS));
    }

    private Node searchByDomainCountryZipCode(Node node) {
        node = node.filter(
                String.format("%s != null && %s != null && %s != null", ATTR_LDC_DOMAIN, ATTR_COUNTRY, ATTR_ZIPCODE),
                new FieldList(ATTR_LDC_DOMAIN, ATTR_COUNTRY, ATTR_ZIPCODE));
        node = node.groupByAndLimit(
                new FieldList(ATTR_LDC_DOMAIN, ATTR_COUNTRY, ATTR_ZIPCODE),
                new FieldList(DataCloudConstants.ATTR_IS_ZIP_PRIMARY_LOCATION), 1, true, true);
        node = node.apply(
                new AMLookupKeyFunction(KEY, ATTR_LDC_DOMAIN, null, ATTR_COUNTRY, null, ATTR_ZIPCODE),
                new FieldList(node.getFieldNames()), new FieldMetadata(KEY, String.class));
        return node.retain(new FieldList(AMLOOKUP_FINAL_FIELDS));
    }

    private Node searchByDomainCountryState(Node node) {
        node = node.filter(
                String.format("%s != null && %s != null && %s != null", ATTR_LDC_DOMAIN, ATTR_COUNTRY, ATTR_STATE),
                new FieldList(ATTR_LDC_DOMAIN, ATTR_COUNTRY, ATTR_STATE));
        node = node.groupByAndLimit(
                new FieldList(ATTR_LDC_DOMAIN, ATTR_COUNTRY, ATTR_STATE),
                new FieldList(DataCloudConstants.ATTR_IS_ST_PRIMARY_LOCATION), 1, true, true);
        node = node.apply(
                new AMLookupKeyFunction(KEY, ATTR_LDC_DOMAIN, null, ATTR_COUNTRY, ATTR_STATE, null),
                new FieldList(node.getFieldNames()), new FieldMetadata(KEY, String.class));
        return node.retain(new FieldList(AMLOOKUP_FINAL_FIELDS));
    }

    private Node searchByDomainCountry(Node node) {
        node = node.filter(String.format("%s != null && %s != null", ATTR_LDC_DOMAIN, ATTR_COUNTRY),
                new FieldList(ATTR_LDC_DOMAIN, ATTR_COUNTRY));
        node = node.groupByAndLimit(new FieldList(ATTR_LDC_DOMAIN, ATTR_COUNTRY),
                new FieldList(DataCloudConstants.ATTR_IS_CTRY_PRIMARY_LOCATION), 1, true, true);
        node = node.apply(
                new AMLookupKeyFunction(KEY, ATTR_LDC_DOMAIN, null, ATTR_COUNTRY, null, null),
                new FieldList(node.getFieldNames()), new FieldMetadata(KEY, String.class));
        return node.retain(new FieldList(AMLOOKUP_FINAL_FIELDS));
    }

    private Node searchByDuns(Node node) {
        node = node.filter(ATTR_LDC_DUNS + " != null", new FieldList(ATTR_LDC_DUNS));
        node = node.groupByAndLimit(new FieldList(ATTR_LDC_DUNS),
                new FieldList(ATTR_IS_PRIMARY_DOMAIN), 1, true, true);
        node = node.apply(
                new AMLookupKeyFunction(KEY, null, ATTR_LDC_DUNS, null, null, null),
                new FieldList(node.getFieldNames()), new FieldMetadata(KEY, String.class));
        return node.retain(new FieldList(AMLOOKUP_FINAL_FIELDS));
    }

    private Node searchByBoth(Node node) {
        node = node.filter(ATTR_LDC_DOMAIN + " != null && " + ATTR_LDC_DUNS + " != null",
                new FieldList(ATTR_LDC_DOMAIN, ATTR_LDC_DUNS));
        node = node.groupByAndLimit(new FieldList(ATTR_LDC_DOMAIN, ATTR_LDC_DUNS), 1);
        node = node.apply(
                new AMLookupKeyFunction(KEY, ATTR_LDC_DOMAIN, ATTR_LDC_DUNS, null, null, null),
                new FieldList(node.getFieldNames()), new FieldMetadata(KEY, String.class));
        return node.retain(new FieldList(AMLOOKUP_FINAL_FIELDS));
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
        return TransformerConfig.class;
    }
}
