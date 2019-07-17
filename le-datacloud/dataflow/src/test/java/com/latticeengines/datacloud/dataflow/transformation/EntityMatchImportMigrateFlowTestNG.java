package com.latticeengines.datacloud.dataflow.transformation;

import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.dataflow.framework.DataCloudDataFlowFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.EntityMatchImportMigrateConfig;

public class EntityMatchImportMigrateFlowTestNG extends DataCloudDataFlowFunctionalTestNGBase {

    @Override
    protected String getFlowBeanName() {
        return EntityMatchImportMigrateFlow.DATAFLOW_BEAN_NAME;
    }

    @Test(groups = "functional")
    public void testAccount() throws Exception {
        TransformationFlowParameters parameters = prepareInput();
        executeDataFlow(parameters);
        verifyResult();
    }

    private void verifyResult() {
        List<GenericRecord> records = readOutput();
        Assert.assertEquals(records.size(), 7);
        for (GenericRecord record : records) {
            Schema schema = record.getSchema();
            Assert.assertNull(schema.getField("AccountId"));
            Assert.assertNull(schema.getField("ContactId"));
            Assert.assertNotNull(schema.getField("CustomerAccountId"));
            Assert.assertNotNull(schema.getField("CustomerContactId"));
            Assert.assertNotNull(schema.getField("SystemAccountId"));
            Assert.assertEquals(schema.getField("SystemAccountId").schema().getTypes().get(0).getType(), Schema.Type.STRING);
            Assert.assertEquals(record.get("CustomerAccountId").toString(), record.get("SystemAccountId").toString());
        }

    }


    private TransformationFlowParameters prepareInput() {
        List<Pair<String, Class<?>>> fields = Arrays.asList( //
                Pair.of("AccountId", String.class), //
                Pair.of("ContactId", String.class),
                Pair.of("ProductId", String.class),
                Pair.of("Key", String.class), //
                Pair.of("Domain", String.class), //
                Pair.of("DUNS", String.class), //
                Pair.of("TransactionDayPeriod", Integer.class)//
        );
        Object[][] data = new Object[][] { //
                { "1", "", "1", "key1", "netapp.com", "DUNS11", 48485 }, //
                { "1", "2", "1", "key1", "netapp.com", "DUNS11", 48185 },
                { "2", "3", "1", "key2", "netapp.com", null, 48485 }, //
                { "3", "5", "1", "key1", "netapp.com", "DUNS11", 48485 }, //
                { "4", "7", "1", "key2", null, "DUNS14", 48485 }, //
                { "5", "9", "1", "key1", null, "DUNS14", 48485 }, //
                { "6", "11", "1", "key2", null, null, 48485 }, //
        };
        uploadDataToSharedAvroInput(data, fields);


        TransformationFlowParameters parameters = new TransformationFlowParameters();
        parameters.setBaseTables(Arrays.asList(AVRO_INPUT));
        EntityMatchImportMigrateConfig config = new EntityMatchImportMigrateConfig();
        config.setRenameMap(ImmutableMap.of("AccountId", "CustomerAccountId", "ContactId", "CustomerContactId"));
        config.setDuplicateMap(ImmutableMap.of("AccountId", "SystemAccountId"));
        parameters.setConfJson(JsonUtils.serialize(config));
        return parameters;
    }
}
