package com.latticeengines.datacloud.dataflow.transformation.am;

import java.util.Arrays;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.dataflow.transformation.ConfigurableFlowBase;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.runtime.cascading.propdata.AMLookupKeyFunction;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.am.AMLookupConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

@Component(AMLookupRebuild.DATAFLOW_BEAN_NAME)
public class AMLookupRebuild extends ConfigurableFlowBase<AMLookupConfig> {

    private AMLookupConfig config;

    public static final String DATAFLOW_BEAN_NAME = "AMLookupRebuildFlow";
    public static final String TRANSFORMER_NAME = "AMLookupRebuild";

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        config = getTransformerConfig(parameters);

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
        String amDomain = config.getDomainField();
        String orbPriDomain = config.getDomainMappingPrimaryDomainField();
        String orbSecDomain = config.getDomainMappingSecondaryDomainField();

        orbSeed = orbSeed.filter(orbPriDomain + " != null", new FieldList(orbPriDomain)) //
                .retain(new FieldList(orbPriDomain, orbSecDomain));

        // join find sec domain for domain == pri domain
        Node hasSd = amSeed.join(new FieldList(amDomain), orbSeed, new FieldList(orbPriDomain), JoinType.INNER);
        hasSd = hasSd.filter(orbSecDomain + " != null", new FieldList(orbSecDomain)) //
                .discard(new FieldList(amDomain));

        Node domains = amSeed.retain(new FieldList(amDomain)) //
                       .groupByAndLimit(new FieldList(amDomain), 1);

        hasSd = hasSd.renamePipe("hasSD") //
                .leftJoin(new FieldList(orbSecDomain), domains, new FieldList(amDomain));

        // secondary not exist in am seed
        Node toAppend = hasSd
                .filter(orbSecDomain + " != null && " + amDomain + " == null", new FieldList(orbSecDomain, amDomain)) //
                .discard(new FieldList(amDomain, orbPriDomain)) //
                .rename(new FieldList(orbSecDomain), new FieldList(amDomain)) //
                .retain(new FieldList(amSeed.getFieldNames()));

        return amSeed.merge(toAppend);
    }

    private Node searchByDomain(Node node) {
        node = node.filter(config.getDomainField() + " != null", new FieldList(config.getDomainField()));
        node = node.groupByAndLimit(new FieldList(config.getDomainField()),
                new FieldList(config.getIsPrimaryLocationField()), 1, true, true);
        node = node.apply(
                new AMLookupKeyFunction(config.getKeyField(), config.getDomainField(), null, null, null,
                        null),
                new FieldList(node.getFieldNames()), new FieldMetadata(config.getKeyField(), String.class));
        return node.retain(new FieldList(config.getLatticeIdField(), config.getKeyField()));
    }

    private Node searchByDomainCountryZipCode(Node node) {
        node = node.filter(String.format("%s != null && %s != null && %s != null", config.getDomainField(),
                config.getCountryField(), config.getZipCodeField()),
                new FieldList(config.getDomainField(), config.getCountryField(), config.getZipCodeField()));
        node = node.groupByAndLimit(
                new FieldList(config.getDomainField(), config.getCountryField(), config.getZipCodeField()),
                new FieldList(DataCloudConstants.ATTR_IS_ZIP_PRIMARY_LOCATION), 1, true, true);
        node = node.apply(
                new AMLookupKeyFunction(config.getKeyField(), config.getDomainField(), null,
                        config.getCountryField(), null, config.getZipCodeField()),
                new FieldList(node.getFieldNames()), new FieldMetadata(config.getKeyField(), String.class));
        return node.retain(new FieldList(config.getLatticeIdField(), config.getKeyField()));
    }

    private Node searchByDomainCountryState(Node node) {
        node = node.filter(String.format("%s != null && %s != null && %s != null", config.getDomainField(),
                config.getCountryField(), config.getStateField()),
                new FieldList(config.getDomainField(), config.getCountryField(), config.getStateField()));
        node = node.groupByAndLimit(
                new FieldList(config.getDomainField(), config.getCountryField(), config.getStateField()),
                new FieldList(DataCloudConstants.ATTR_IS_ST_PRIMARY_LOCATION), 1, true, true);
        node = node.apply(
                new AMLookupKeyFunction(config.getKeyField(), config.getDomainField(), null,
                        config.getCountryField(), config.getStateField(), null),
                new FieldList(node.getFieldNames()), new FieldMetadata(config.getKeyField(), String.class));
        return node.retain(new FieldList(config.getLatticeIdField(), config.getKeyField()));
    }

    private Node searchByDomainCountry(Node node) {
        node = node.filter(String.format("%s != null && %s != null", config.getDomainField(), config.getCountryField()),
                new FieldList(config.getDomainField(), config.getCountryField()));
        node = node.groupByAndLimit(new FieldList(config.getDomainField(), config.getCountryField()),
                new FieldList(DataCloudConstants.ATTR_IS_CTRY_PRIMARY_LOCATION), 1, true, true);
        node = node.apply(
                new AMLookupKeyFunction(config.getKeyField(), config.getDomainField(), null,
                        config.getCountryField(), null, null),
                new FieldList(node.getFieldNames()), new FieldMetadata(config.getKeyField(), String.class));
        return node.retain(new FieldList(config.getLatticeIdField(), config.getKeyField()));
    }

    private Node searchByDuns(Node node) {
        node = node.filter(config.getDunsField() + " != null", new FieldList(config.getDunsField()));
        node = node.groupByAndLimit(new FieldList(config.getDunsField()),
                new FieldList(config.getIsPrimaryDomainField()), 1, true, true);
        node = node.apply(
                new AMLookupKeyFunction(config.getKeyField(), null, config.getDunsField(), null, null, null),
                new FieldList(node.getFieldNames()), new FieldMetadata(config.getKeyField(), String.class));
        return node.retain(new FieldList(config.getLatticeIdField(), config.getKeyField()));
    }

    private Node searchByBoth(Node node) {
        node = node.filter(config.getDomainField() + " != null && " + config.getDunsField() + " != null",
                new FieldList(config.getDomainField(), config.getDunsField()));
        node = node.groupByAndLimit(new FieldList(config.getDomainField(), config.getDunsField()), 1);
        node = node.apply(
                new AMLookupKeyFunction(config.getKeyField(), config.getDomainField(), config.getDunsField(),
                        null, null, null),
                new FieldList(node.getFieldNames()), new FieldMetadata(config.getKeyField(), String.class));
        return node.retain(new FieldList(config.getLatticeIdField(), config.getKeyField()));
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
        return AMLookupConfig.class;
    }
}
