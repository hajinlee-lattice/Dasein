package com.latticeengines.datacloud.etl.transformation.service.impl;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.source.impl.GeneralSource;
import com.latticeengines.datacloud.dataflow.transformation.ConsolidateReportFlow;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.ConsolidateReportConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ConsolidateReportTestNG extends PipelineTransformationTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(ConsolidateReportTestNG.class);

    private GeneralSource account = new GeneralSource("Account");
    private GeneralSource accountReport = new GeneralSource("AccountReport");
    private GeneralSource contact = new GeneralSource("Contact");
    private GeneralSource contactReport = new GeneralSource("ContactReport");
    private GeneralSource product = new GeneralSource("Product");
    private GeneralSource productReport = new GeneralSource("ProductReport");
    private GeneralSource transaction = new GeneralSource("Transaction");
    private GeneralSource transactionReport = new GeneralSource("TransactionReport");
    private GeneralSource source = productReport;

    private String targetVersion = "2017-11-01_00-00-00_UTC";

    @Test(groups = "functional")
    public void testTransformation() {
        prepareAccount();
        prepareContact();
        prepareProduct();
        prepareTransaction();
        TransformationProgress progress = createNewProgress();
        progress = transformData(progress);
        finish(progress);
        confirmIntermediateSource(accountReport, null);
        confirmIntermediateSource(contactReport, null);
        confirmIntermediateSource(productReport, null);
        confirmIntermediateSource(transactionReport, null);
        confirmResultFile(progress);
        cleanupProgressTables();
    }

    @Override
    protected String getTargetSourceName() {
        return source.getSourceName();
    }

    @Override
    protected Source getSource() {
        return source;
    }

    @Override
    protected String getPathToUploadBaseData() {
        return hdfsPathBuilder.constructSnapshotDir(source.getSourceName(), targetVersion).toString();
    }

    @Override
    protected PipelineTransformationConfiguration createTransformationConfiguration() {
        PipelineTransformationConfiguration configuration = new PipelineTransformationConfiguration();
        configuration.setName("ConsolidateReport");
        configuration.setVersion(targetVersion);

        TransformationStepConfig step0 = new TransformationStepConfig();
        List<String> baseSources = new ArrayList<>();
        baseSources.add(account.getSourceName());
        step0.setBaseSources(baseSources);
        step0.setTransformer(ConsolidateReportFlow.TRANSFORMER_NAME);
        step0.setTargetSource(accountReport.getSourceName());
        String confParamStr0 = getReportConfig(BusinessEntity.Account);
        step0.setConfiguration(confParamStr0);

        TransformationStepConfig step1 = new TransformationStepConfig();
        baseSources = new ArrayList<>();
        baseSources.add(contact.getSourceName());
        baseSources.add(account.getSourceName());
        step1.setBaseSources(baseSources);
        step1.setTransformer(ConsolidateReportFlow.TRANSFORMER_NAME);
        step1.setTargetSource(contactReport.getSourceName());
        String confParamStr1 = getReportConfig(BusinessEntity.Contact);
        step1.setConfiguration(confParamStr1);

        TransformationStepConfig step2 = new TransformationStepConfig();
        baseSources = new ArrayList<>();
        baseSources.add(product.getSourceName());
        step2.setBaseSources(baseSources);
        step2.setTransformer(ConsolidateReportFlow.TRANSFORMER_NAME);
        step2.setTargetSource(productReport.getSourceName());
        String confParamStr2 = getReportConfig(BusinessEntity.Product);
        step2.setConfiguration(confParamStr2);

        TransformationStepConfig step3 = new TransformationStepConfig();
        baseSources = new ArrayList<>();
        baseSources.add(transaction.getSourceName());
        step3.setBaseSources(baseSources);
        step3.setTransformer(ConsolidateReportFlow.TRANSFORMER_NAME);
        step3.setTargetSource(transactionReport.getSourceName());
        String confParamStr3 = getReportConfig(BusinessEntity.Transaction);
        step3.setConfiguration(confParamStr3);

        // -----------
        List<TransformationStepConfig> steps = new ArrayList<TransformationStepConfig>();
        steps.add(step0);
        steps.add(step1);
        steps.add(step2);
        steps.add(step3);

        // -----------
        configuration.setSteps(steps);
        configuration.setKeepTemp(true);
        return configuration;
    }

    private String getReportConfig(BusinessEntity entity) {
        ConsolidateReportConfig config = new ConsolidateReportConfig();
        config.setEntity(entity);
        return JsonUtils.serialize(config);
    }

    private void prepareAccount() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of(InterfaceName.LatticeAccountId.name(), String.class));
        schema.add(Pair.of(InterfaceName.AccountId.name(), String.class));
        schema.add(Pair.of(InterfaceName.CDLCreatedTime.name(), Long.class));

        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        Object[][] accountData;
        try {
            accountData = new Object[][] { { "123", "1", ((Date) formatter.parse("2017-11-10")).getTime() }, //
                    { "456", "2", ((Date) formatter.parse("2017-10-08")).getTime() }, //
                    { "789", "4", null }, //
                    { null, "5", ((Date) formatter.parse("2017-11-11")).getTime() }, //
            };
        } catch (ParseException e) {
            throw new RuntimeException("Fail to prepare account data", e);
        }

        uploadBaseSourceData(account.getSourceName(), baseSourceVersion, schema, accountData);
    }

    private void prepareContact() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of(InterfaceName.Id.name(), String.class));
        schema.add(Pair.of(InterfaceName.AccountId.name(), String.class));
        schema.add(Pair.of(InterfaceName.CDLCreatedTime.name(), Long.class));

        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        Object[][] contactData;
        try {
            contactData = new Object[][] { { "123", "5", ((Date) formatter.parse("2017-10-08")).getTime() }, //
                    { "456", null, null }, //
                    { null, "6", null } };
        } catch (ParseException e) {
            throw new RuntimeException("Fail to prepare contact data", e);
        }

        uploadBaseSourceData(contact.getSourceName(), baseSourceVersion, schema, contactData);
    }

    private void prepareProduct() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of(InterfaceName.ProductId.name(), String.class));
        schema.add(Pair.of(InterfaceName.CDLCreatedTime.name(), Long.class));

        DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        Object[][] productData;
        try {
            productData = new Object[][] { { "123", ((Date) formatter.parse("2017-11-10")).getTime() }, //
                    { null, ((Date) formatter.parse("2017-11-11")).getTime() }, //
            };
        } catch (ParseException e) {
            throw new RuntimeException("Fail to prepare product data", e);
        }

        uploadBaseSourceData(product.getSourceName(), baseSourceVersion, schema, productData);
    }

    private void prepareTransaction() {
        List<Pair<String, Class<?>>> schema = new ArrayList<>();
        schema.add(Pair.of(InterfaceName.TransactionId.name(), String.class));
        schema.add(Pair.of(InterfaceName.CDLCreatedTime.name(), Long.class));

        Object[][] transactionData = {};

        uploadBaseSourceData(transaction.getSourceName(), baseSourceVersion, schema, transactionData);
    }

    @Override
    protected void verifyIntermediateResult(String source, String version, Iterator<GenericRecord> records) {
        log.info(String.format("Start to verify intermediate source %s", source));
        switch (source) {
        case "AccountReport":
            verifyAccountReport(records);
            break;
        case "ContactReport":
            verifyContactReport(records);
            break;
        case "ProductReport":
            verifyProductReport(records);
            break;
        case "TransactionReport":
            verifyTransactionReport(records);
            break;
        default:
            throw new UnsupportedOperationException("Unknown source: " + source);
        }
    }

    private void verifyAccountReport(Iterator<GenericRecord> records) {
        String accountReportStr = records.next().get(InterfaceName.ConsolidateReport.name()).toString();
        try {
            ObjectMapper om = JsonUtils.getObjectMapper();
            JsonNode node = om.readTree(accountReportStr);
            JsonNode report = node.get(BusinessEntity.Account.name());
            Assert.assertEquals(report.get(ConsolidateReportFlow.REPORT_TOPIC_NEW).asInt(), 2);
            Assert.assertEquals(report.get(ConsolidateReportFlow.REPORT_TOPIC_UPDATE).asInt(), 2);
            Assert.assertEquals(report.get(ConsolidateReportFlow.REPORT_TOPIC_UNMATCH).asInt(), 1);
        } catch (IOException e) {
            throw new RuntimeException("Fail to read parse report: " + accountReportStr);
        }
    }

    private void verifyContactReport(Iterator<GenericRecord> records) {
        String contactReportStr = records.next().get(InterfaceName.ConsolidateReport.name()).toString();
        try {
            ObjectMapper om = JsonUtils.getObjectMapper();
            JsonNode node = om.readTree(contactReportStr);
            JsonNode report = node.get(BusinessEntity.Contact.name());
            Assert.assertEquals(report.get(ConsolidateReportFlow.REPORT_TOPIC_NEW).asInt(), 0);
            Assert.assertEquals(report.get(ConsolidateReportFlow.REPORT_TOPIC_UPDATE).asInt(), 3);
            Assert.assertEquals(report.get(ConsolidateReportFlow.REPORT_TOPIC_MATCH).asInt(), 1);
        } catch (IOException e) {
            throw new RuntimeException("Fail to read parse report: " + contactReportStr);
        }
    }

    private void verifyProductReport(Iterator<GenericRecord> records) {
        String accountReportStr = records.next().get(InterfaceName.ConsolidateReport.name()).toString();
        try {
            ObjectMapper om = JsonUtils.getObjectMapper();
            JsonNode node = om.readTree(accountReportStr);
            JsonNode report = node.get(BusinessEntity.Product.name());
            Assert.assertEquals(report.get(ConsolidateReportFlow.REPORT_TOPIC_NEW).asInt(), 2);
            Assert.assertEquals(report.get(ConsolidateReportFlow.REPORT_TOPIC_UPDATE).asInt(), 0);
        } catch (IOException e) {
            throw new RuntimeException("Fail to read parse report: " + accountReportStr);
        }
    }

    private void verifyTransactionReport(Iterator<GenericRecord> records) {
        String accountReportStr = records.next().get(InterfaceName.ConsolidateReport.name()).toString();
        try {
            ObjectMapper om = JsonUtils.getObjectMapper();
            JsonNode node = om.readTree(accountReportStr);
            JsonNode report = node.get(BusinessEntity.Transaction.name());
            Assert.assertEquals(report.get(ConsolidateReportFlow.REPORT_TOPIC_NEW).asInt(), 0);
        } catch (IOException e) {
            throw new RuntimeException("Fail to read parse report: " + accountReportStr);
        }
    }

    @Override
    protected void verifyResultAvroRecords(Iterator<GenericRecord> records) {

    }
}
