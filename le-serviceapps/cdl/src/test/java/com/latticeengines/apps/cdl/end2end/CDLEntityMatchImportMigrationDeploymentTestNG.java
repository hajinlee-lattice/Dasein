package com.latticeengines.apps.cdl.end2end;

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.cdl.ImportMigrateTracking;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ImportActionConfiguration;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.cdl.MigrateTrackingProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public class CDLEntityMatchImportMigrationDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(CDLEntityMatchImportMigrationDeploymentTestNG.class);

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private MigrateTrackingProxy migrateTrackingProxy;

    @Inject
    private ActionProxy actionProxy;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @BeforeClass(groups = { "end2end" })
    public void setup() throws Exception {
        setupEnd2EndTestEnvironment();
        resumeCheckpoint(ProcessTransactionDeploymentTestNG.CHECK_POINT);
    }

    @Test(groups = "end2end")
    public void runTest() {
        migrateImport();
        verifyMigration();
    }

    private void verifyMigration() {
        DataFeed dataFeed = dataFeedProxy.getDataFeed(mainCustomerSpace);

        List<ImportMigrateTracking> migrateTrackings = migrateTrackingProxy.getAllMigrateTracking(mainCustomerSpace);
        Optional<ImportMigrateTracking> optionalTracking =
                migrateTrackings.stream().filter(tracking -> tracking.getStatus() == ImportMigrateTracking.Status.COMPLETED).findFirst();
        Assert.assertTrue(optionalTracking.isPresent());
        ImportMigrateTracking importMigrateTracking = optionalTracking.get();
        List<String> migratedDataFeedTasks = new ArrayList<>();
        if (StringUtils.isNotEmpty(importMigrateTracking.getReport().getOutputAccountTaskId())) {
            migratedDataFeedTasks.add(importMigrateTracking.getReport().getOutputAccountTaskId());
        }
        if (StringUtils.isNotEmpty(importMigrateTracking.getReport().getOutputContactTaskId())) {
            migratedDataFeedTasks.add(importMigrateTracking.getReport().getOutputContactTaskId());
        }
        if (StringUtils.isNotEmpty(importMigrateTracking.getReport().getOutputTransactionTaskId())) {
            migratedDataFeedTasks.add(importMigrateTracking.getReport().getOutputTransactionTaskId());
        }
        Assert.assertTrue(dataFeed.getTasks().stream().map(DataFeedTask::getUniqueId).collect(Collectors.toSet()).containsAll(migratedDataFeedTasks));

        DataFeedTask accountTask = dataFeedProxy.getDataFeedTask(mainCustomerSpace,
                importMigrateTracking.getReport().getOutputAccountTaskId());
        Assert.assertNull(accountTask.getImportTemplate().getAttribute(InterfaceName.AccountId.name()));
        Assert.assertNotNull(accountTask.getImportTemplate().getAttribute(InterfaceName.CustomerAccountId.name()));
        Assert.assertNull(accountTask.getImportTemplate().getAttribute(InterfaceName.CDLCreatedTime.name()));
        Assert.assertNull(accountTask.getImportTemplate().getAttribute(InterfaceName.CDLUpdatedTime.name()));

        List<Action> contactImportActions = actionProxy.getActionsByPids(mainCustomerSpace,
                Collections.singletonList(importMigrateTracking.getReport().getContactActionId()));
        Assert.assertNotNull(contactImportActions);
        Assert.assertEquals(contactImportActions.size(), 1);
        ImportActionConfiguration importActionConfiguration =
                (ImportActionConfiguration) contactImportActions.get(0).getActionConfiguration();
        Assert.assertNotNull(importActionConfiguration);
        Assert.assertTrue(importActionConfiguration.getImportCount() > 0);
        long recordsCountFromAction = importActionConfiguration.getImportCount();
        long recordsCountFromAvro = 0;
        List<String> dataTables = importActionConfiguration.getRegisteredTables();
        for (String tableName : dataTables) {
            Table dataTable = metadataProxy.getTable(mainCustomerSpace, tableName);
            Assert.assertNull(dataTable.getAttribute(InterfaceName.ContactId.name()));
            Assert.assertNotNull(dataTable.getAttribute(InterfaceName.CustomerContactId.name()));
            Assert.assertNull(dataTable.getAttribute(InterfaceName.CDLCreatedTime.name()));
            Assert.assertNull(dataTable.getAttribute(InterfaceName.CDLUpdatedTime.name()));
            List<String> paths = new ArrayList<>();
            for (Extract e : dataTable.getExtracts()) {
                paths.add(e.getPath());
            }
            List<GenericRecord> records = AvroUtils.getDataFromGlob(yarnConfiguration, paths);
            Assert.assertTrue(CollectionUtils.isNotEmpty(records));
            Schema schema = records.get(0).getSchema();
            Assert.assertNull(schema.getField("ContactId"));
            Assert.assertNotNull(schema.getField("CustomerContactId"));
            recordsCountFromAvro += records.size();
        }
        Assert.assertEquals(recordsCountFromAction, recordsCountFromAvro);
    }

    private void migrateImport() {
        Table accountBatchStore = dataCollectionProxy.getTable(mainCustomerSpace,
                BusinessEntity.Account.getBatchStore());
        Table contactBatchStore = dataCollectionProxy.getTable(mainCustomerSpace,
                BusinessEntity.Contact.getBatchStore());
        Table transactionRawStore = dataCollectionProxy.getTable(mainCustomerSpace,
                TableRoleInCollection.ConsolidatedRawTransaction);
        Table accountTemplate = TableUtils.clone(accountBatchStore, "AccountTemplate");
        accountTemplate.removeAttribute(InterfaceName.CDLCreatedTime.name());
        accountTemplate.removeAttribute(InterfaceName.CDLUpdatedTime.name());
        accountTemplate.removeAttribute(InterfaceName.InternalId.name());
        accountTemplate.setExtracts(new ArrayList<>());
        createDataFeedTask(accountTemplate, BusinessEntity.Account.name(),
                EntityType.Accounts.getDefaultFeedTypeName());
        Table contactTemplate = TableUtils.clone(contactBatchStore, "ContactTemplate");
        contactTemplate.removeAttribute(InterfaceName.CDLCreatedTime.name());
        contactTemplate.removeAttribute(InterfaceName.CDLUpdatedTime.name());
        contactTemplate.removeAttribute(InterfaceName.InternalId.name());
        contactTemplate.setExtracts(new ArrayList<>());
        createDataFeedTask(contactTemplate, BusinessEntity.Contact.name(),
                EntityType.Contacts.getDefaultFeedTypeName());
        Table transactionTemplate = TableUtils.clone(transactionRawStore, "TransactionTemplate");
        transactionTemplate.removeAttribute(InterfaceName.CDLCreatedTime.name());
        transactionTemplate.removeAttribute(InterfaceName.CDLUpdatedTime.name());
        transactionTemplate.removeAttribute(InterfaceName.InternalId.name());
        transactionTemplate.setExtracts(new ArrayList<>());
        createDataFeedTask(transactionTemplate, BusinessEntity.Transaction.name(),
                EntityType.ProductPurchases.getDefaultFeedTypeName());

        ApplicationId appId = cdlProxy.migrateImport(mainCustomerSpace, "E2E_USER");
        log.info("MigrateImport application id: " + appId.toString());
        com.latticeengines.domain.exposed.workflow.JobStatus completedStatus = waitForWorkflowStatus(appId.toString(),
                false);
        assertEquals(completedStatus, JobStatus.COMPLETED);
    }

    private void createDataFeedTask(Table template, String entity, String feedType) {
        String newTaskId = NamingUtils.uuid("DataFeedTask");
        DataFeedTask dataFeedTask = new DataFeedTask();
        dataFeedTask.setUniqueId(newTaskId);
        dataFeedTask.setImportTemplate(template);
        dataFeedTask.setStatus(DataFeedTask.Status.Active);
        dataFeedTask.setEntity(entity);
        dataFeedTask.setFeedType(feedType);
        dataFeedTask.setSource(SourceType.FILE.getName());
        dataFeedTask.setActiveJob("Not specified");
        dataFeedTask.setSourceConfig("Not specified");
        dataFeedTask.setStartTime(new Date());
        dataFeedTask.setLastImported(new Date(0L));
        dataFeedTask.setLastUpdated(new Date());
        dataFeedProxy.createDataFeedTask(mainCustomerSpace, dataFeedTask);
    }

}
