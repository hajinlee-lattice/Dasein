package com.latticeengines.datacloud.dataflow.transformation;

import java.util.ArrayList;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.AccountMasterLookupDomainBuffer;
import com.latticeengines.dataflow.runtime.cascading.propdata.AccountMasterLookupKeyFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.AccountMasterLookupRebuildConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.tuple.Fields;

@Component("accountMasterLookupRebuildFlow")
public class AccountMasterLookupRebuildFlow extends ConfigurableFlowBase<AccountMasterLookupRebuildConfig> {

    private AccountMasterLookupRebuildConfig config;

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        config = getTransformerConfig(parameters);

        Node accountMasterSeed = addSource(parameters.getBaseTables().get(0));
        /*
        Node orbCacheSeedSecondaryDomainMapping = addSource(parameters.getBaseTables().get(1));

        orbCacheSeedSecondaryDomainMapping = orbCacheSeedSecondaryDomainMapping.rename(
                new FieldList(config.getDomainMappingPrimaryDomainField()),
                new FieldList("_" + config.getDomainMappingPrimaryDomainField() + "_"));

        Node accountMasterSeedSecondaryDomainCleaned = accountMasterSeed.join(new FieldList(config.getDomainField()),
                orbCacheSeedSecondaryDomainMapping, new FieldList(config.getDomainMappingSecondaryDomainField()),
                JoinType.LEFT);
        accountMasterSeedSecondaryDomainCleaned = accountMasterSeedSecondaryDomainCleaned.filter(
                config.getDomainMappingSecondaryDomainField() + " == null",
                new FieldList(config.getDomainMappingSecondaryDomainField()));
        accountMasterSeedSecondaryDomainCleaned = accountMasterSeedSecondaryDomainCleaned
                .retain(new FieldList(accountMasterSeed.getFieldNames()));

        Node accountMasterSeedWithSecondaryDomain1 = accountMasterSeed.join(new FieldList(config.getDomainField()),
                orbCacheSeedSecondaryDomainMapping,
                new FieldList("_" + config.getDomainMappingPrimaryDomainField() + "_"),
                JoinType.INNER);

        Node accountMasterSeedWithSecondaryDomain = accountMasterSeedWithSecondaryDomain1;

        accountMasterSeedWithSecondaryDomain = accountMasterSeedWithSecondaryDomain.filter(
                config.getDomainMappingSecondaryDomainField() + " != null",
                new FieldList(config.getDomainMappingSecondaryDomainField()));
        accountMasterSeedWithSecondaryDomain = accountMasterSeedWithSecondaryDomain.discard(
                new FieldList(config.getDomainField(), "_" + config.getDomainMappingPrimaryDomainField() + "_"));
        accountMasterSeedWithSecondaryDomain = accountMasterSeedWithSecondaryDomain
                .rename(new FieldList(config.getDomainMappingSecondaryDomainField()), new FieldList(config.getDomainField()));

        accountMasterSeedWithSecondaryDomain = accountMasterSeedWithSecondaryDomain
                .retain(new FieldList(accountMasterSeedSecondaryDomainCleaned.getFieldNames()));
        Node accountMasterSeedSecondaryDomainCleanedWithSecondaryDomain = accountMasterSeedSecondaryDomainCleaned
                .merge(accountMasterSeedWithSecondaryDomain);
        */
        Node searchByDuns = searchByDuns(accountMasterSeed);
        Node searchByDomain = searchByDomain(accountMasterSeed);
        Node searchByDomainCountryZipCode = searchByDomainCountryZipCode(accountMasterSeed);
        Node searchByDomainCountryState = searchByDomainCountryState(accountMasterSeed);
        Node searchByDomainCountry = searchByDomainCountry(accountMasterSeed);
        Node searchByBoth = searchByBoth(accountMasterSeed);
        return searchByDomain.merge(searchByDomainCountryZipCode).merge(searchByDomainCountryState)
                .merge(searchByDomainCountry).merge(searchByDuns).merge(searchByBoth);
    }

    private Node searchByDomain(Node node) {
        node = node.filter(config.getDomainField() + " != null", new FieldList(config.getDomainField()));
        List<String> returnedFields = prepareReturnedFieldsForSearchByDomain();
        List<FieldMetadata> returnedMetadata = prepareFieldsMetadataForSearchByDomain();
        node = node.groupByAndBuffer(new FieldList(config.getDomainField()), new AccountMasterLookupDomainBuffer(
                new Fields(returnedFields.toArray(new String[returnedFields.size()])), returnedFields,
                        config.getDunsField(), config.getDuDunsField(), config.getGuDunsField(),
                        config.getEmployeeField(), config.getSalesVolumeUsDollars(), config.getIsPrimaryLocationField(),
                        config.getCountryField()),
                        returnedMetadata);
        node = node.apply(
                new AccountMasterLookupKeyFunction(config.getKeyField(), config.getDomainField(), null, null, null,
                        null),
                new FieldList(node.getFieldNames()), new FieldMetadata(config.getKeyField(), String.class));
        return node.retain(new FieldList(config.getLatticeIdField(), config.getKeyField()));
    }

    private Node searchByDomainCountryZipCode(Node node) {
        node = node.filter(String.format("%s != null && %s != null && %s != null", config.getDomainField(),
                        config.getCountryField(), config.getZipCodeField()),
                new FieldList(config.getDomainField(), config.getCountryField(), config.getZipCodeField()));
        List<String> returnedFields = prepareReturnedFieldsForSearchByDomain();
        List<FieldMetadata> returnedMetadata = prepareFieldsMetadataForSearchByDomain();
        node = node.groupByAndBuffer(
                new FieldList(config.getDomainField(), config.getCountryField(), config.getZipCodeField()),
                new AccountMasterLookupDomainBuffer(
                        new Fields(returnedFields.toArray(new String[returnedFields.size()])), returnedFields,
                        config.getDunsField(), config.getDuDunsField(), config.getGuDunsField(),
                        config.getEmployeeField(), config.getSalesVolumeUsDollars(), config.getIsPrimaryLocationField(),
                        config.getCountryField()),
                returnedMetadata);
        node = node.apply(
                new AccountMasterLookupKeyFunction(config.getKeyField(), config.getDomainField(), null,
                        config.getCountryField(), null, config.getZipCodeField()),
                new FieldList(node.getFieldNames()), new FieldMetadata(config.getKeyField(), String.class));
        return node.retain(new FieldList(config.getLatticeIdField(), config.getKeyField()));
    }

    private Node searchByDomainCountryState(Node node) {
        node = node.filter(String.format("%s != null && %s != null && %s != null", config.getDomainField(),
                        config.getCountryField(), config.getStateField()),
                new FieldList(config.getDomainField(), config.getCountryField(), config.getStateField()));
        List<String> returnedFields = prepareReturnedFieldsForSearchByDomain();
        List<FieldMetadata> returnedMetadata = prepareFieldsMetadataForSearchByDomain();
        node = node.groupByAndBuffer(
                new FieldList(config.getDomainField(), config.getCountryField(), config.getStateField()),
                new AccountMasterLookupDomainBuffer(
                        new Fields(returnedFields.toArray(new String[returnedFields.size()])), returnedFields,
                        config.getDunsField(), config.getDuDunsField(), config.getGuDunsField(),
                        config.getEmployeeField(), config.getSalesVolumeUsDollars(), config.getIsPrimaryLocationField(),
                        config.getCountryField()),
                returnedMetadata);
        node = node.apply(
                new AccountMasterLookupKeyFunction(config.getKeyField(), config.getDomainField(), null,
                        config.getCountryField(), config.getStateField(), null),
                new FieldList(node.getFieldNames()), new FieldMetadata(config.getKeyField(), String.class));
        return node.retain(new FieldList(config.getLatticeIdField(), config.getKeyField()));
    }

    private Node searchByDomainCountry(Node node) {
        node = node.filter(String.format("%s != null && %s != null", config.getDomainField(), config.getCountryField()),
                new FieldList(config.getDomainField(), config.getCountryField()));
        List<String> returnedFields = prepareReturnedFieldsForSearchByDomain();
        List<FieldMetadata> returnedMetadata = prepareFieldsMetadataForSearchByDomain();
        node = node.groupByAndBuffer(new FieldList(config.getDomainField(), config.getCountryField()),
                new AccountMasterLookupDomainBuffer(
                        new Fields(returnedFields.toArray(new String[returnedFields.size()])), returnedFields,
                        config.getDunsField(), config.getDuDunsField(), config.getGuDunsField(),
                        config.getEmployeeField(), config.getSalesVolumeUsDollars(), config.getIsPrimaryLocationField(),
                        config.getCountryField()),
                returnedMetadata);
        node = node.apply(
                new AccountMasterLookupKeyFunction(config.getKeyField(), config.getDomainField(), null,
                        config.getCountryField(), null, null),
                new FieldList(node.getFieldNames()), new FieldMetadata(config.getKeyField(), String.class));
        return node.retain(new FieldList(config.getLatticeIdField(), config.getKeyField()));
    }

    private List<String> prepareReturnedFieldsForSearchByDomain() {
        List<String> fields = new ArrayList<String>();
        fields.add(config.getLatticeIdField());
        fields.add(config.getDomainField());
        fields.add(config.getCountryField());
        fields.add(config.getStateField());
        fields.add(config.getZipCodeField());
        return fields;
    }

    private List<FieldMetadata> prepareFieldsMetadataForSearchByDomain() {
        List<FieldMetadata> fieldMetadata = new ArrayList<FieldMetadata>();
        fieldMetadata.add(new FieldMetadata(config.getLatticeIdField(), Long.class));
        fieldMetadata.add(new FieldMetadata(config.getDomainField(), String.class));
        fieldMetadata.add(new FieldMetadata(config.getCountryField(), String.class));
        fieldMetadata.add(new FieldMetadata(config.getStateField(), String.class));
        fieldMetadata.add(new FieldMetadata(config.getZipCodeField(), String.class));
        return fieldMetadata;
    }

    private Node searchByDuns(Node node) {
        node = node.filter(config.getDunsField() + " != null", new FieldList(config.getDunsField()));
        node = node.groupByAndLimit(new FieldList(config.getDunsField()),
                new FieldList(config.getIsPrimaryDomainField()), 1, true, true);
        node = node.apply(
                new AccountMasterLookupKeyFunction(config.getKeyField(), null, config.getDunsField(), null, null, null),
                new FieldList(node.getFieldNames()), new FieldMetadata(config.getKeyField(), String.class));
        return node.retain(new FieldList(config.getLatticeIdField(), config.getKeyField()));
    }

    private Node searchByBoth(Node node) {
        node = node.filter(config.getDomainField() + " != null && " + config.getDunsField() + " != null",
                new FieldList(config.getDomainField(), config.getDunsField()));
        node = node.groupByAndLimit(new FieldList(config.getDomainField(), config.getDunsField()), 1);
        node = node.apply(
                new AccountMasterLookupKeyFunction(config.getKeyField(), config.getDomainField(), config.getDunsField(),
                        null, null, null),
                new FieldList(node.getFieldNames()), new FieldMetadata(config.getKeyField(), String.class));
        return node.retain(new FieldList(config.getLatticeIdField(), config.getKeyField()));
    }

    @Override
    public String getDataFlowBeanName() {
        return "accountMasterLookupRebuildFlow";
    }

    @Override
    public String getTransformerName() {
        return "accountMasterLookupRebuildTransformer";
    }

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return AccountMasterLookupRebuildConfig.class;
    }
}
