package com.latticeengines.datacloud.etl.transformation.service.impl.atlas;

import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.ENTITY_ID_FIELD;
import static com.latticeengines.domain.exposed.datacloud.match.MatchConstants.ENTITY_NAME_FIELD;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.City;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CompanyName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ContactId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Country;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CustomerAccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.DUNS;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.EntityId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.LatticeAccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PhoneNumber;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PostalCode;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.State;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Website;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableSet;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.atlas.ExtractEmbeddedEntityTable;
import com.latticeengines.datacloud.etl.transformation.service.impl.PipelineTransformationTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.atlas.ExtractEmbeddedEntityTableConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

public class ExtractEmbeddedEntityTableTestNG extends PipelineTransformationTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ExtractEmbeddedEntityTableTestNG.class);

    private GeneralSource entityIds = new GeneralSource("EntityIds");
    private GeneralSource embeddedAcctTable1 = new GeneralSource("EmbeddedAcctTable1");
    private GeneralSource embeddedAcctTable2 = new GeneralSource("EmbeddedAcctTable2");
    private GeneralSource acctTable1 = new GeneralSource("AccountTable1");
    private GeneralSource acctTable2 = new GeneralSource("AccountTable2");
    private GeneralSource source = acctTable2;

    @Test(groups = "functional")
    public void testTransformation() {
        prepareEntityIds();
        prepareEmbeddedAcctTable1();
        prepareEmbeddedAcctTable2();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        confirmIntermediateSource(acctTable1, targetVersion);
        confirmIntermediateSource(acctTable2, targetVersion);
        cleanupProgressTables();
    }

    @Override
    protected String getTargetSourceName() {
        return source.getSourceName();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
        configuration.setName("ExtractEmbeddedEntityTable");
        configuration.setVersion(targetVersion);

        TransformationStepConfig step1 = new TransformationStepConfig();
        List<String> baseSources = new ArrayList<>();
        baseSources.add(entityIds.getSourceName());
        baseSources.add(embeddedAcctTable1.getSourceName());
        step1.setBaseSources(baseSources);
        step1.setTransformer(ExtractEmbeddedEntityTable.TRANSFORMER_NAME);
        step1.setTargetSource(acctTable1.getSourceName());
        step1.setConfiguration(getConfig());

        TransformationStepConfig step2 = new TransformationStepConfig();
        baseSources = new ArrayList<>();
        baseSources.add(entityIds.getSourceName());
        baseSources.add(embeddedAcctTable2.getSourceName());
        step2.setBaseSources(baseSources);
        step2.setTransformer(ExtractEmbeddedEntityTable.TRANSFORMER_NAME);
        step2.setTargetSource(acctTable2.getSourceName());
        step2.setConfiguration(getConfig());

        // -----------
        List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
        steps.add(step1);
        steps.add(step2);

        // -----------
        configuration.setSteps(steps);
        return configuration;
    }

    private String getConfig() {
        ExtractEmbeddedEntityTableConfig config = new ExtractEmbeddedEntityTableConfig();
        config.setEntity(Account.name());
        config.setEntityIdFld(AccountId.name());
        config.setSystemIdFlds(Arrays.asList(CustomerAccountId.name(), "NonExistedId"));
        return JsonUtils.serialize(config);
    }

    // Schema: EntityName, EntityId
    private Object[][] entityIdsData = new Object[][] { //
            { Account.name(), "A01" }, //
            { Account.name(), "A02" }, //
            { Account.name(), "A03" }, //
            { Account.name(), "A04" }, //
            { Account.name(), "A05" }, //
            // Account EntityId not existed in embedded entity table, thus
            // not expected to show up in target source
            { Account.name(), "A06" }, //
    };

    private void prepareEntityIds() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of(ENTITY_NAME_FIELD, String.class));
        schema.add(Pair.of(ENTITY_ID_FIELD, String.class));

        uploadBaseSourceData(entityIds.getSourceName(), baseSourceVersion, schema, entityIdsData);
    }

    // Test Scenario: Partial Account match fields (No SystemIds)
    private String[] embeddedAcctTableSchema1 = { //
            // Contact EntityId
            EntityId.name(), ContactId.name(),
            // Matched Account EntityId
            AccountId.name(),
            // Matched Account LatticeAccountId
            LatticeAccountId.name(),
            // Account match key fields
            Website.name(), DUNS.name() };

    private String[] expectedAcctTableSchema1 = { //
            // Account EntityId
            EntityId.name(), AccountId.name(),
            // Matched Account LatticeAccountId
            LatticeAccountId.name(),
            // Account match key fields
            Website.name(), DUNS.name()
    };

    // Schema: EntityId, ContactId, AccountId, LatticeAccountId, Website, DUNS
    private Object[][] embeddedAcctTableData1 = new Object[][] { //
            { "C01", "C01", "A01", "LDC01", "dom1.com", "duns1" }, //
            { "C02", "C02", "A01", "LDC02", null, "duns1" }, //
            { "C03", "C03", "A01", "LDC03", "dom1.com", null }, //
            { "C04", "C04", "A01", "LDC04", null, null }, //
            { "C05", "C05", "A02", "LDC05", "dom2.com", "duns2" }, //
            { "C06", "C06", "A03", "LDC06", null, "duns3" }, //
            { "C07", "C07", "A04", "LDC07", "dom4.com", null }, //
            { "C08", "C08", "A05", "LDC08", null, null }, //
            // Account EntityId not existed in EntityIds table, thus not
            // expected to show up in target source
            { "C09", "C09", "A01_NonExist", "LDC09", "dom1.com", "duns1" }, //
            { "C10", "C10", "A02_NonExist", "LDC10", "dom2.com", null }, //
            { "C11", "C11", "A03_NonExist", "LDC11", null, "duns3" }, //
            { "C12", "C12", "A04_NonExist", "LDC12", null, null }, //
    };

    // Based on PA use case, this table is actually match result of Contact
    // match in AllocateId mode
    private void prepareEmbeddedAcctTable1() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        for (String field : embeddedAcctTableSchema1) {
            schema.add(Pair.of(field, String.class));
        }

        uploadBaseSourceData(embeddedAcctTable1.getSourceName(), baseSourceVersion, schema, embeddedAcctTableData1);
    }

    // Test Scenario: Complete Account match fields (With SystemIds)
    private String[] embeddedAcctTableSchema2 = {
            // Contact EntityId
            EntityId.name(), ContactId.name(),
            // Matched Account EntityId
            AccountId.name(),
            // Matched Account LatticeAccountId
            LatticeAccountId.name(),
            // Account match key fields
            CustomerAccountId.name(), Website.name(), DUNS.name(), CompanyName.name(), City.name(), State.name(),
            Country.name(), PostalCode.name(), PhoneNumber.name()
    };

    private String[] expectedAcctTableSchema2 = {
            // Account EntityId
            EntityId.name(), AccountId.name(),
            // Matched Account LatticeAccountId
            LatticeAccountId.name(),
            // Account match key fields
            CustomerAccountId.name(), Website.name(), DUNS.name(), CompanyName.name(), City.name(), State.name(),
            Country.name(), PostalCode.name(), PhoneNumber.name() //
    };

    private Object[][] embeddedAcctTableData2 = new Object[][] { //
            { "C01", "C01", "A01", "LDC01", "CA01", "dom1.com", "duns1", "name1", "city1", "state1", "country1",
                    "zipcode1", "phone1" }, //
            { "C02", "C02", "A01", "LDC02", "CA01", null, "duns1", "name1", "city1", "state1", "country1", null, null }, //
            { "C03", "C03", "A01", "LDC03", "CA01", "dom1.com", null, null, null, null, null, "zipcode1", "phone1" }, //
            { "C04", "C04", "A01", "LDC04", "CA01", null, null, null, null, null, null, null, null }, //
            { "C05", "C05", "A02", "LDC05", "CA02", "dom2.com", "duns2", "name2", "city2", "state2", "country2",
                    "zipcode2", "phone2" }, //
            { "C06", "C06", "A03", "LDC06", "CA03", null, "duns3", "name3", "city3", "state3", "country3", null, null }, //
            { "C07", "C07", "A04", "LDC07", "CA04", "dom4.com", null, null, null, null, null, "zipcode4", "phone4" }, //
            { "C08", "C08", "A05", "LDC08", "CA05", null, null, null, null, null, null, null, null }, //
            // Account EntityId not existed in EntityIds table, thus not
            // expected to show up in target source
            { "C09", "C09", "A01_NonExist", "LDC09", "CA01", "dom1.com", "duns1", "name1", "city1", "state1",
                    "country1", "zipcode1", "phone1" }, //
            { "C10", "C10", "A02_NonExist", "LDC10", "CA02", "dom2.com", null, "name2", "city2", "state2", "country2",
                    null, null }, //
            { "C11", "C11", "A03_NonExist", "LDC11", "CA03", null, "duns3", null, null, null, null, "zipcode3",
                    "phone3" }, //
            { "C12", "C12", "A04_NonExist", "LDC12", "CA04", null, null, null, null, null, null, null, null }, //
    };

    // Based on PA use case, this table is actually match result of Contact
    // match in AllocateId mode
    private void prepareEmbeddedAcctTable2() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        for (String field : embeddedAcctTableSchema2) {
            schema.add(Pair.of(field, String.class));
        }

        uploadBaseSourceData(embeddedAcctTable2.getSourceName(), baseSourceVersion, schema, embeddedAcctTableData2);
    }

    @Override
    protected void verifyIntermediateResult(String source, String version, Iterator<GenericRecord> records) {
        log.info("Verifying intermediate source " + source);
        switch (source) {
        case "AccountTable1":
            verifyIntermediateSource(records, Arrays.asList(expectedAcctTableSchema1),
                    Arrays.asList(embeddedAcctTableSchema1), embeddedAcctTableData1);
            break;
        case "AccountTable2":
            verifyIntermediateSource(records, Arrays.asList(expectedAcctTableSchema2),
                    Arrays.asList(embeddedAcctTableSchema2), embeddedAcctTableData2);
            break;
        default:
            throw new IllegalArgumentException("Unknown intermediate source " + source);
        }
    }

    private void verifyIntermediateSource(Iterator<GenericRecord> records, List<String> expectedSchema,
            List<String> inputSchema, Object[][] inputData) {
        Collections.sort(expectedSchema);
        // Expected field name -> Index of the field in INPUT data
        Map<String, Integer> schemaIdxes = expectedSchema.stream()
                .collect(Collectors.toMap(field -> field,
                        field -> inputSchema.indexOf(EntityId.name().equals(field) ? AccountId.name() : field)));

        Set<String> expectedAIDs = ImmutableSet.of("A01", "A02", "A03", "A04", "A05");
        // RecordId (Use LatticeAccountId which is designed to be unique in test
        // case) -> record data
        // data[3]: RecordId; data[2]: Account EntityId
        Map<String, Object[]> expectedData = Arrays.stream(inputData).filter(data -> expectedAIDs.contains(data[2]))//
                .collect(Collectors.toMap(data -> (String) data[3], data -> data));

        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info(record.toString());
            // Verify schema
            List<String> schema = record.getSchema().getFields().stream().map(Schema.Field::name)
                    .collect(Collectors.toList());
            Collections.sort(schema);
            Assert.assertEquals(schema, expectedSchema);
            // Verify data
            String recordId = record.get(LatticeAccountId.name()).toString();
            Assert.assertTrue(expectedData.containsKey(recordId));
            schemaIdxes.entrySet().forEach(ent -> {
                isObjEquals(record.get(ent.getKey()), expectedData.get(recordId)[ent.getValue()]);
            });
            expectedData.remove(recordId);
        }
        Assert.assertTrue(expectedData.isEmpty());
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
    }

}
