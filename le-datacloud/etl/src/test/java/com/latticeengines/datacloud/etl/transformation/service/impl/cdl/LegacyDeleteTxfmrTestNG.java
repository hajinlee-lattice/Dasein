package com.latticeengines.datacloud.etl.transformation.service.impl.cdl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.etl.transformation.service.impl.PipelineTransformationService;
import com.latticeengines.datacloud.etl.transformation.service.impl.PipelineTransformationTestNGBase;
import com.latticeengines.datacloud.etl.transformation.transformer.impl.cdl.LegacyDeleteTxfmr;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.spark.cdl.LegacyDeleteJobConfig;

public class LegacyDeleteTxfmrTestNG extends PipelineTransformationTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(LegacyDeleteTxfmrTestNG.class);

    private final GeneralSource DeleteSrc = new GeneralSource("Delete");
    private final GeneralSource BaseSrc = new GeneralSource("Base");

    protected static final String CLEANUPBASE = "cleanup_base";
    protected static final String AVRO_INPUT = "AvroInput";
    protected static final String AVRO_DIR = "/tmp/avro";

    @Inject
    private PipelineTransformationService transformationService;

    @Test(groups = "functional", enabled = false)
    public void testWithAvro() {
        prepareAvroInput();
        runProcess();
    }

    @Test(groups = "functional", enabled = false)
    public void testAccount() {
        prepareInput();
        runProcess();
    }

    @Test(groups = "functional", enabled = false)
    public void testACPDTransaction() throws Exception {
        prepareInput();
        runProcess();
    }

    @Test(groups = "functional")
    public void testMinDateTransaction() throws Exception {
        prepareInput();
        runProcess();
    }

    @Test(groups = "functional", enabled = false)
    public void testMinDateAccountTransaction() throws Exception {
        prepareInput();
        runProcess();
    }

    private void runProcess() {
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmResultFile(progress);
        cleanupProgressTables();
    }


    @Override
    protected String getTargetSourceName() {
        return "legacyDeleteTest";
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
//        return prepareConfig(BusinessEntity.Transaction, CleanupOperationType
//                .BYUPLOAD_MINDATE);
//        return prepareConfig(BusinessEntity.Account, CleanupOperationType.BYUPLOAD_ID);
//        return prepareConfig(BusinessEntity.Transaction, CleanupOperationType
//                .BYUPLOAD_ACPD);
        return prepareConfig(BusinessEntity.Transaction, CleanupOperationType
                .BYUPLOAD_MINDATE);
//        return prepareConfig(BusinessEntity.Transaction, CleanupOperationType
//                .BYUPLOAD_MINDATEANDACCOUNT);

    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {
//        verifyWithAvro(records);
//        verifyByIDResult(records);
//        verifyByACPDResult(records);
        verifyByMinDateResult(records);
//        verifyByMinDateAccountResult(records);
    }

    private void prepareAvroInput() {
        uploadBaseSourceFile(BaseSrc.getSourceName(), "CleanupBase.avro", baseSourceVersion);
        uploadBaseSourceFile(DeleteSrc.getSourceName(), "CleanupTemplate.avro", baseSourceVersion);
    }

    private LegacyDeleteJobConfig.JoinedColumns getJoinedColumns(CleanupOperationType type) {
        LegacyDeleteJobConfig.JoinedColumns joinedColumns = new LegacyDeleteJobConfig.JoinedColumns();
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

    private PipelineTransformationConfiguration prepareConfig(BusinessEntity entity, CleanupOperationType type) {
        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
        configuration.setName("legacyDelete");
        configuration.setVersion(targetVersion);

        TransformationStepConfig step1 = new TransformationStepConfig();
        List<String> baseSources = new ArrayList<>();
        baseSources.add(DeleteSrc.getSourceName());
        baseSources.add(BaseSrc.getSourceName());
        step1.setBaseSources(baseSources);
        step1.setTransformer(LegacyDeleteTxfmr.TRANSFORMER_NAME);
        step1.setTargetSource(getTargetSourceName());
        LegacyDeleteJobConfig jobConfig = new LegacyDeleteJobConfig();
        jobConfig.setDeleteSourceIdx(0);
        jobConfig.setBusinessEntity(entity);
        jobConfig.setOperationType(type);
        jobConfig.setJoinedColumns(getJoinedColumns(type));
        step1.setConfiguration(JsonUtils.serialize(jobConfig));

        // -----------
        List<TransformationStepConfig> steps = new ArrayList<>();
        steps.add(step1);

        // -----------
        configuration.setSteps(steps);
        return configuration;
    }

    private void verifyWithAvro(Iterator<GenericRecord> records) {
        while (records.hasNext()) {
            GenericRecord record = records.next();
            if (record.get("AccountId").toString().equals("0012400001HD0COAA1") &&
                    record.get("ContactId").toString().equals("") &&
                    record.get("ProductId").toString().equals("A80D4770376C1226C47617C071324C0B") &&
                    record.get("TransactionDayPeriod").toString().equals("48929")) {
                System.out.println(record.toString());
            }
        }
    }

    private void verifyByIDResult(Iterator<GenericRecord> records) {
        boolean contains1 = false;
        boolean contains4 = false;
        int count = 0;
        while (records.hasNext()) {
            count++;
            GenericRecord record =records.next();
            log.info("record is {}.", record.getSchema());
            if (record.get("AccountId").toString().equals("1")) {
                contains1 = true;
            }
            if (record.get("AccountId").toString().equals("4")) {
                contains4 = true;
            }
        }
        Assert.assertFalse(contains1 || contains4);
        Assert.assertEquals(count, 4);
    }

    private void verifyByACPDResult(Iterator<GenericRecord> records) {
        boolean contains = false;
        int count = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info("record is {}.", record.getSchema());
            count++;
            if (record.get("AccountId").toString().equals("1") &&
                    record.get("ContactId").toString().equals("") &&
                    record.get("ProductId").toString().equals("1")) {
                contains = true;
            }
        }
        Assert.assertFalse(contains);
        Assert.assertEquals(count, 6);
    }

    private void verifyByMinDateResult(Iterator<GenericRecord> records) {
        int count = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            count++;
            log.info("record is {}.", record.getSchema());
            int time = Integer.parseInt(record.get("TransactionDayPeriod").toString());
            Assert.assertTrue(time < 48285);
        }
        Assert.assertEquals(count, 1);
    }

    private void verifyByMinDateAccountResult(Iterator<GenericRecord> records) {
        int time1 = Integer.MAX_VALUE;
        boolean contains4 = false;
        int count = 0;
        while (records.hasNext()) {
            GenericRecord record = records.next();
            log.info("record is {}.", record.getSchema());
            count++;
            if (record.get("AccountId").toString().equals("1")) {
                time1 = Integer.parseInt(record.get("TransactionDayPeriod").toString());
            }
            if (record.get("AccountId").toString().equals("4")) {
                contains4 = true;
            }
        }
        Assert.assertEquals(count, 5);
        Assert.assertTrue(time1 < 48285);
        Assert.assertFalse(contains4);
    }

    private void prepareInput() {
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
        uploadBaseSourceData(BaseSrc.getSourceName(), baseSourceVersion, fields, data);
        uploadBaseSourceData(DeleteSrc.getSourceName(), baseSourceVersion, fields2, data2);
    }
}
