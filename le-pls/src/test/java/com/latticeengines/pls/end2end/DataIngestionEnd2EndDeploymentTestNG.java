package com.latticeengines.pls.end2end;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.CEAttr;
import static org.testng.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

public class DataIngestionEnd2EndDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DataIngestionEnd2EndDeploymentTestNG.class);

    private static final String COLLECTION_DATE_FORMAT = "yyyy-MM-dd-HH-mm-ss";

    @Autowired
    private WorkflowProxy workflowProxy;

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    @Autowired
    private DataFeedProxy dataFeedProxy;

    @Autowired
    private CDLProxy cdlProxy;

    @Autowired
    protected Configuration yarnConfiguration;

    private Tenant mainTenant;

    @BeforeClass(groups = { "deployment.cdl" })
    public void setup() throws Exception {
        log.info("Bootstrapping test tenants using tenant console ...");

        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG);
        mainTenant = testBed.getMainTestTenant();
        testBed.excludeTestTenantsForCleanup(Collections.singletonList(mainTenant));

        log.info("Test environment setup finished.");
        createDataFeed();
    }

    @Test(groups = "deployment.cdl")
    public void testEndToEnd() throws Exception {
        importData();
        initialConsolidate();
        firstProfile();

        importSecondData();
        secondConsolidate();
        secondProfile();

        thirdConsolidate();

        querySegment();
    }

    private void importData() throws Exception {
        // TODO: after we make change to enforce single extract per table, we can simplify the test and remove third consolidate
        mockAvroData(0, 300);
        Thread.sleep(2000);
        mockAvroData(300, 200);
        Thread.sleep(2000);
        mockAvroData(500, 200);
        dataFeedProxy.updateDataFeedStatus(mainTenant.getId(), DataFeed.Status.InitialLoaded.getName());
    }

    private void initialConsolidate() {
        log.info("Start consolidating ...");
        ApplicationId appId = cdlProxy.consolidate(mainTenant.getId());
        JobStatus completedStatus = waitForWorkflowStatus(workflowProxy, appId.toString(), false);
        assertEquals(completedStatus, JobStatus.COMPLETED);

        long numAccounts = countTableRole(BusinessEntity.Account.getBatchStore());
        Assert.assertEquals(numAccounts, 300);
        long numContacts = countTableRole(BusinessEntity.Contact.getBatchStore());
        Assert.assertEquals(numContacts, 300);
    }

    private void firstProfile() throws IOException {
        log.info("Start profiling ...");
        ApplicationId appId = cdlProxy.profile(mainTenant.getId());
        JobStatus completedStatus = waitForWorkflowStatus(workflowProxy, appId.toString(), false);
        assertEquals(completedStatus, JobStatus.COMPLETED);
        verifyFirstProfile();
    }

    private void verifyFirstProfile() throws IOException {
        String customerSpace = CustomerSpace.parse(mainTenant.getId()).toString();
        Table bucketedAccountTable = dataCollectionProxy.getTable(customerSpace,
                BusinessEntity.Account.getServingStore());
        Assert.assertNotNull(bucketedAccountTable);
        List<Attribute> attributes = bucketedAccountTable.getAttributes();
        for (Attribute attribute : attributes) {
            Assert.assertFalse(attribute.getName().contains(CEAttr),
                    "Should not have encoded attr " + attribute.getName() + " in expanded table.");
        }
        StatisticsContainer statisticsContainer = dataCollectionProxy.getStats(customerSpace);
        Assert.assertNotNull(statisticsContainer);
        // save stats to a local json to help create verifications
        File statsJson = new File("stats.json");
        FileUtils.deleteQuietly(statsJson);
        FileUtils.write(statsJson, JsonUtils.pprint(statisticsContainer));

        Table bucketedContactTable = dataCollectionProxy.getTable(customerSpace,
                BusinessEntity.Contact.getServingStore());
        Assert.assertNotNull(bucketedContactTable);
        attributes = bucketedContactTable.getAttributes();
        for (Attribute attribute : attributes) {
            Assert.assertFalse(attribute.getName().contains(CEAttr),
                    "Should not have encoded attr " + attribute.getName() + " in expanded table.");
        }
    }

    private void importSecondData() throws Exception {
        mockAvroData(700, 300);
    }

    private void secondConsolidate() {
        log.info("Start second consolidating ...");
        ApplicationId appId = cdlProxy.consolidate(mainTenant.getId());
        JobStatus completedStatus = waitForWorkflowStatus(workflowProxy, appId.toString(), false);
        assertEquals(completedStatus, JobStatus.COMPLETED);

        long numAccounts = countTableRole(BusinessEntity.Account.getBatchStore());
        Assert.assertEquals(numAccounts, 700);
        long numContacts = countTableRole(BusinessEntity.Contact.getBatchStore());
        Assert.assertEquals(numContacts, 700);
    }

    private void secondProfile() {
        log.info("Start second profiling ...");
        ApplicationId appId = cdlProxy.profile(mainTenant.getId());
        JobStatus completedStatus = waitForWorkflowStatus(workflowProxy, appId.toString(), false);
        assertEquals(completedStatus, JobStatus.COMPLETED);
        verifySecondProfile();
    }

    private void verifySecondProfile() {
        String customerSpace = CustomerSpace.parse(mainTenant.getId()).toString();
        Table bucketedAccountTable = dataCollectionProxy.getTable(customerSpace,
                BusinessEntity.Account.getServingStore());
        Assert.assertNotNull(bucketedAccountTable);
        Table bucketedContactTable = dataCollectionProxy.getTable(customerSpace,
                BusinessEntity.Contact.getServingStore());
        Assert.assertNotNull(bucketedContactTable);
    }

    private void thirdConsolidate() {
        log.info("Start third consolidating ...");
        ApplicationId appId = cdlProxy.consolidate(mainTenant.getId());
        JobStatus completedStatus = waitForWorkflowStatus(workflowProxy, appId.toString(), false);
        assertEquals(completedStatus, JobStatus.COMPLETED);

        long numAccounts = countTableRole(BusinessEntity.Account.getBatchStore());
        Assert.assertEquals(numAccounts, 1000);
        long numContacts = countTableRole(BusinessEntity.Contact.getBatchStore());
        Assert.assertEquals(numContacts, 1000);
    }

    private void querySegment() {
    }

    private void mockAvroData(int offset, int limit) throws IOException {
        mockAvroDataInternal(BusinessEntity.Account, offset, limit);
        mockAvroDataInternal(BusinessEntity.Contact, offset, limit);
    }

    private void mockAvroDataInternal(BusinessEntity entity, int offset, int limit) throws IOException {
        CustomerSpace customerSpace = CustomerSpace.parse(mainTenant.getId());

        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), "VisiDB", "Query",
                entity.name());
        Table importTemplate;
        if (dataFeedTask == null) {
            Schema schema = getSchema(entity);
            importTemplate = MetadataConverter.getTable(schema, new ArrayList<>(), null, null, false);
            importTemplate.setTableType(TableType.IMPORTTABLE);
            if (BusinessEntity.Account.equals(entity)) {
                importTemplate.setName(SchemaInterpretation.Account.name());
            } else {
                importTemplate.setName(SchemaInterpretation.Contact.name());
            }
            dataFeedTask = new DataFeedTask();
            dataFeedTask.setImportTemplate(importTemplate);
            dataFeedTask.setStatus(DataFeedTask.Status.Active);
            dataFeedTask.setEntity(entity.name());
            dataFeedTask.setFeedType("Query");
            dataFeedTask.setSource("VisiDB");
            dataFeedTask.setActiveJob("Not specified");
            dataFeedTask.setSourceConfig("Not specified");
            dataFeedTask.setStartTime(new Date());
            dataFeedTask.setLastImported(new Date(0L));
            dataFeedTask.setUniqueId(NamingUtils.uuid("DataFeedTask"));
            dataFeedProxy.createDataFeedTask(customerSpace.toString(), dataFeedTask);
        } else {
            importTemplate = dataFeedTask.getImportTemplate();
        }

        String targetPath = uploadMockDataWithModifiedSchema(entity, offset, limit);
        String defaultFS = yarnConfiguration.get(FileSystem.FS_DEFAULT_NAME_KEY);
        String hdfsUri = String.format("%s%s/%s", defaultFS, targetPath, "*.avro");
        Extract e = createExtract(hdfsUri, (long) limit);
        dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), "VisiDB", "Query", entity.name());
        dataFeedProxy.registerExtract(customerSpace.toString(), dataFeedTask.getUniqueId(), importTemplate.getName(),
                e);
    }

    private Schema getSchema(BusinessEntity entity) {
        Schema schema;
        try {
            InputStream schemaIs = Thread.currentThread().getContextClassLoader()
                    .getResourceAsStream("com/latticeengines/pls/end2end/cdl/Account.avsc");
            String schemaStr = IOUtils.toString(schemaIs, Charset.forName("UTF-8"));
            switch (entity) {
            case Contact:
                schemaStr = schemaStr.replace("\"External_ID\"", "\"" + InterfaceName.LEContactIDLong.name() + "\"");
                schemaStr = schemaStr.replace("\"LEAccountIDLong\"", "\"" + InterfaceName.AccountId.name() + "\"");
                break;
            case Account:
            default:
            }

            schema = new Schema.Parser().parse(schemaStr);
            switch (entity) {
            case Contact:
                boolean hasLEContactIDLong = schema.getFields().stream().map(Schema.Field::name)
                        .anyMatch(n -> InterfaceName.LEContactIDLong.name().equals(n));
                boolean hasAccountId = schema.getFields().stream().map(Schema.Field::name)
                        .anyMatch(n -> InterfaceName.AccountId.name().equals(n));
                Assert.assertTrue(hasLEContactIDLong);
                Assert.assertTrue(hasAccountId);
                break;
            case Account:
                boolean hasLEAccountIDLong = schema.getFields().stream().map(Schema.Field::name)
                        .anyMatch(n -> InterfaceName.LEAccountIDLong.name().equals(n));
                Assert.assertTrue(hasLEAccountIDLong);
                break;
            default:
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to prepare avro schema for " + entity);
        }
        return schema;
    }

    private String uploadMockDataWithModifiedSchema(BusinessEntity entity, int offset, int limit) {
        Schema schema = getSchema(entity);
        CustomerSpace customerSpace = CustomerSpace.parse(mainTenant.getId());
        String targetPath = String.format("%s/%s/DataFeed1/DataFeed1-" + entity + "/Extracts/%s",
                PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace).toString(),
                SourceType.VISIDB.getName(), new SimpleDateFormat(COLLECTION_DATE_FORMAT).format(new Date()));
        InputStream dataIs = Thread.currentThread().getContextClassLoader()
                .getResourceAsStream("com/latticeengines/pls/end2end/cdl/Account.avro");
        try {
            List<GenericRecord> records = AvroUtils.readFromInputStream(dataIs);
            AvroUtils.writeToHdfsFile(yarnConfiguration, schema, targetPath + "/part-00000.avro", records.subList(offset, offset + limit),
                    true);
            log.info("Uploaded " + limit + " records to " + targetPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to upload avro for " + entity);
        }
        return targetPath;
    }

    private Extract createExtract(String path, long processedRecords) {
        Extract e = new Extract();
        e.setName(StringUtils.substringAfterLast(path, "/"));
        e.setPath(PathUtils.stripoutProtocol(path));
        e.setProcessedRecords(processedRecords);
        String dateTime = StringUtils.substringBetween(path, "/Extracts/", "/");
        SimpleDateFormat f = new SimpleDateFormat(COLLECTION_DATE_FORMAT);
        try {
            e.setExtractionTimestamp(f.parse(dateTime).getTime());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return e;
    }

    private void createDataFeed() {
        dataFeedProxy.getDataFeed(mainTenant.getId());
        Table importTable = new Table();
        importTable.setName("importTable");
        importTable.setDisplayName(importTable.getName());
        importTable.setTenant(mainTenant);
        Table dataTable = new Table();
        dataTable.setName("dataTable");
        dataTable.setDisplayName(dataTable.getName());
        dataTable.setTenant(mainTenant);
    }

    private long countTableRole(TableRoleInCollection role) {
        CustomerSpace customerSpace = CustomerSpace.parse(mainTenant.getId());
        Table table = dataCollectionProxy.getTable(customerSpace.toString(), role);
        if (table == null) {
            Assert.fail("Cannot find table in role " + role);
        }
        return table.getExtracts().get(0).getProcessedRecords();
    }
}
