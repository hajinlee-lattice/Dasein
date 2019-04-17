package com.latticeengines.datacloud.dataflow.transformation;

import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.dataflow.framework.DataCloudDataFlowFunctionalTestNGBase;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.CleanupConfig;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class CleanupTestNG extends DataCloudDataFlowFunctionalTestNGBase {

    protected static final String CLEANUPBASE = "cleanup_base";


    @Override
    protected String getFlowBeanName() {
        return CleanupFlow.DATAFLOW_BEAN_NAME;
    }

    @Test(groups = "functional")
    public void testWithAvro() {
        TransformationFlowParameters parameters = prepareAvroInput(BusinessEntity.Transaction, CleanupOperationType
                .BYUPLOAD_MINDATE);
        executeDataFlow(parameters);
        verifyWithAvro();
    }

    private void verifyWithAvro() {
        List<GenericRecord> records = readOutput();
        for (GenericRecord record : records) {
            if (record.get("AccountId").toString().equals("0012400001HD0COAA1") &&
                    record.get("ContactId").toString().equals("") &&
                    record.get("ProductId").toString().equals("A80D4770376C1226C47617C071324C0B") &&
                    record.get("TransactionDayPeriod").toString().equals("48929")) {
                System.out.println(record.toString());
            }
        }

    }

    @Test(groups = "functional")
    public void testAccount() throws Exception {
        TransformationFlowParameters parameters = prepareInput(BusinessEntity.Account, CleanupOperationType.BYUPLOAD_ID);
        executeDataFlow(parameters);
        verifyByIDResult();
    }

    @Test(groups = "functional")
    public void testACPDTransaction() throws Exception {
        TransformationFlowParameters parameters = prepareInput(BusinessEntity.Transaction, CleanupOperationType
                .BYUPLOAD_ACPD);
        executeDataFlow(parameters);
        verifyByACPDResult();
    }

    @Test(groups = "functional")
    public void testMinDateTransaction() throws Exception {
        TransformationFlowParameters parameters = prepareInput(BusinessEntity.Transaction, CleanupOperationType
                .BYUPLOAD_MINDATE);
        executeDataFlow(parameters);
        verifyByMinDateResult();
    }

    @Test(groups = "functional")
    public void testMinDateAccountTransaction() throws Exception {
        TransformationFlowParameters parameters = prepareInput(BusinessEntity.Transaction, CleanupOperationType
                .BYUPLOAD_MINDATEANDACCOUNT);
        executeDataFlow(parameters);
        verifyByMinDateAccountResult();
    }

    private TransformationFlowParameters prepareAvroInput(BusinessEntity entity, CleanupOperationType type) {
        URL baseAvro = ClassLoader.getSystemResource("cleanup/CleanupBase.avro");
        URL templateAvro = ClassLoader.getSystemResource("cleanup/CleanupTemplate.avro");
        copyAvro(templateAvro.getFile(), CLEANUPBASE, "/tmp/cleanup_base");
        copyAvro(baseAvro.getFile(), AVRO_INPUT, AVRO_DIR);
        TransformationFlowParameters parameters = new TransformationFlowParameters();
        parameters.setBaseTables(Arrays.asList(CLEANUPBASE, AVRO_INPUT));
        CleanupConfig config = new CleanupConfig();
        config.setBaseJoinedColumns(getJoinedColumns(type));
        config.setDeleteJoinedColumns(getJoinedColumns(type));
        config.setBusinessEntity(entity);
        config.setOperationType(type);
        parameters.setConfJson(JsonUtils.serialize(config));
        return parameters;
    }

    private TransformationFlowParameters prepareInput(BusinessEntity entity, CleanupOperationType type) {
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

        List<Pair<String, Class<?>>> fields2 = Arrays.asList( //
                Pair.of("AccountId", String.class), //
                Pair.of("ContactId", String.class),
                Pair.of("ProductId", String.class),
                Pair.of("TransactionDayPeriod", Integer.class)//
        );
        Object[][] data2 = new Object[][] { //
                {"1", "", "1", 48485 }, //
                {"1", "3", "1", 48285 },
                {"4", "7", "1", 48385 }, //
        };
        uploadAvro(data2, fields2, CLEANUPBASE, "/tmp/cleanup_base");
        uploadDataToSharedAvroInput(data, fields);


        TransformationFlowParameters parameters = new TransformationFlowParameters();
        parameters.setBaseTables(Arrays.asList(CLEANUPBASE, AVRO_INPUT));
        CleanupConfig config = new CleanupConfig();
        config.setBaseJoinedColumns(getJoinedColumns(type));
        config.setDeleteJoinedColumns(getJoinedColumns(type));
        config.setBusinessEntity(entity);
        config.setOperationType(type);
        parameters.setConfJson(JsonUtils.serialize(config));
        return parameters;
    }

    private CleanupConfig.JoinedColumns getJoinedColumns(CleanupOperationType type) {
        CleanupConfig.JoinedColumns joinedColumns = new CleanupConfig.JoinedColumns();
        switch (type) {
        case BYUPLOAD_ID:
            joinedColumns.setAccountId("AccountId");
            break;
        case BYUPLOAD_ACPD:
            joinedColumns.setAccountId("AccountId");
            joinedColumns.setContactId("ContactId");
            joinedColumns.setProductId("ProductId");
            joinedColumns.setTransactionTime("TransactionDayPeriod");
            break;
        case BYUPLOAD_MINDATE:
            joinedColumns.setTransactionTime("TransactionDayPeriod");
            break;
        case BYUPLOAD_MINDATEANDACCOUNT:
            joinedColumns.setAccountId("AccountId");
            joinedColumns.setTransactionTime("TransactionDayPeriod");
            break;
        default:
            break;
        }
        return joinedColumns;
    }

    @Override
    protected Map<String, String> extraSourcePaths() {
        return Collections.singletonMap(CLEANUPBASE, "/tmp/cleanup_base/" + CLEANUPBASE + ".avro");
    }

    private void verifyByIDResult() {
        List<GenericRecord> records = readOutput();
        Assert.assertEquals(records.size(), 4);
        boolean contains1 = false;
        boolean contains4 = false;
        for (GenericRecord record : records) {
            if (record.get("AccountId").toString().equals("1")) {
                contains1 = true;
            }
            if (record.get("AccountId").toString().equals("4")) {
                contains4 = true;
            }
        }
        Assert.assertFalse(contains1 || contains4);
    }

    private void verifyByACPDResult() {
        List<GenericRecord> records = readOutput();
        Assert.assertEquals(records.size(), 6);
        boolean contains = false;
        for (GenericRecord record : records) {
            if (record.get("AccountId").toString().equals("1") &&
                    record.get("ContactId").toString().equals("") &&
                    record.get("ProductId").toString().equals("1")) {
                contains = true;
            }
        }
        Assert.assertFalse(contains);
    }

    private void verifyByMinDateResult() {
        List<GenericRecord> records = readOutput();
        Assert.assertEquals(records.size(), 1);
        int time = Integer.parseInt(records.get(0).get("TransactionDayPeriod").toString());
        Assert.assertTrue(time < 48285);
    }

    private void verifyByMinDateAccountResult() {
        List<GenericRecord> records = readOutput();
        Assert.assertEquals(records.size(), 5);
        int time1 = Integer.MAX_VALUE;
        boolean contains4 = false;
        for (GenericRecord record : records) {
            if (record.get("AccountId").toString().equals("1")) {
                time1 = Integer.parseInt(record.get("TransactionDayPeriod").toString());
            }
            if (record.get("AccountId").toString().equals("4")) {
                contains4 = true;
            }
        }
        Assert.assertTrue(time1 < 48285);
        Assert.assertFalse(contains4);
    }
}
