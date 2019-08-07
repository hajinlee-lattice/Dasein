package com.latticeengines.apps.cdl.end2end;

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.map.HashedMap;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.cdl.ConvertBatchStoreToImportRequest;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.ImportActionConfiguration;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public class ConvertBatchStoreToImportDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ConvertBatchStoreToImportDeploymentTestNG.class);

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private ActionProxy actionProxy;

    @Inject
    private DataFeedProxy dataFeedProxy;

    private static final String TEMPLATE_NAME = "ConvertedAccountTemplate";
    private static final String EXTRA_ATTR_NAME = "Extra_Duplicate_Id";

    @BeforeClass(groups = { "end2end" })
    public void setup() throws Exception {
        setupEnd2EndTestEnvironment();
        resumeCheckpoint(ProcessTransactionDeploymentTestNG.CHECK_POINT);
    }

    @Test(groups = "end2end")
    public void runTest() {
        convertAccountBatchStore();
        verifyConvert();
    }

    private void verifyConvert() {
        // verify datafeed task:
        DataFeed dataFeed = dataFeedProxy.getDataFeed(mainCustomerSpace);
        Assert.assertNotNull(dataFeed);
        Assert.assertTrue(CollectionUtils.isNotEmpty(dataFeed.getTasks()));
        DataFeedTask dataFeedTask = dataFeed.getTasks().stream().filter(task -> task.getEntity().equals(
                "Account")).findFirst().get();
        Assert.assertNull(dataFeedTask.getImportTemplate().getAttribute(InterfaceName.AccountId.name()));
        Assert.assertNotNull(dataFeedTask.getImportTemplate().getAttribute(InterfaceName.CustomerAccountId.name()));
        Assert.assertNotNull(dataFeedTask.getImportTemplate().getAttribute(EXTRA_ATTR_NAME));
        Assert.assertNull(dataFeedTask.getImportTemplate().getAttribute(InterfaceName.CDLCreatedTime.name()));
        Assert.assertNull(dataFeedTask.getImportTemplate().getAttribute(InterfaceName.CDLUpdatedTime.name()));
        // verify actions:
        List<Action> actions = actionProxy.getActions(mainCustomerSpace);
        Assert.assertTrue(CollectionUtils.isNotEmpty(actions));
        Action importAction =
                actions.stream().filter(action -> ActionType.CDL_DATAFEED_IMPORT_WORKFLOW.equals(action.getType())).findFirst().get();
        ImportActionConfiguration importActionConfiguration = (ImportActionConfiguration) importAction.getActionConfiguration();
        Assert.assertNotNull(importActionConfiguration);
        Assert.assertTrue(importActionConfiguration.getImportCount() > 0);
        long recordsCountFromAction = importActionConfiguration.getImportCount();
        long recordsCountFromAvro = 0;
        List<String> dataTables = importActionConfiguration.getRegisteredTables();
        for (String tableName : dataTables) {
            Table dataTable = metadataProxy.getTable(mainCustomerSpace, tableName);
            Assert.assertNull(dataTable.getAttribute(InterfaceName.AccountId.name()));
            Assert.assertNotNull(dataTable.getAttribute(InterfaceName.CustomerAccountId.name()));
            Assert.assertNotNull(dataTable.getAttribute(EXTRA_ATTR_NAME));
            Assert.assertNull(dataTable.getAttribute(InterfaceName.CDLCreatedTime.name()));
            Assert.assertNull(dataTable.getAttribute(InterfaceName.CDLUpdatedTime.name()));
            List<String> paths = new ArrayList<>();
            for (Extract e : dataTable.getExtracts()) {
                paths.add(e.getPath());
            }
            List<GenericRecord> records = AvroUtils.getDataFromGlob(yarnConfiguration, paths);
            Assert.assertTrue(CollectionUtils.isNotEmpty(records));
            Schema schema = records.get(0).getSchema();
            Assert.assertNull(schema.getField("AccountId"));
            Assert.assertNotNull(schema.getField("CustomerAccountId"));
            Assert.assertNotNull(schema.getField(EXTRA_ATTR_NAME));
            recordsCountFromAvro += records.size();
        }
        Assert.assertEquals(recordsCountFromAction, recordsCountFromAvro);
    }

    private void convertAccountBatchStore() {
        Table batchStore = dataCollectionProxy.getTable(mainCustomerSpace, BusinessEntity.Account.getBatchStore());
        Table templateTable = TableUtils.clone(batchStore, TEMPLATE_NAME);
        Assert.assertNotNull(templateTable.getAttribute(InterfaceName.AccountId));
        Assert.assertNull(templateTable.getAttribute(InterfaceName.CustomerAccountId));
        Attribute accountId = templateTable.getAttribute(InterfaceName.AccountId);
        templateTable.removeAttribute(InterfaceName.AccountId.name());
        templateTable.removeAttribute(InterfaceName.CDLCreatedTime.name());
        templateTable.removeAttribute(InterfaceName.CDLUpdatedTime.name());
        templateTable.addAttribute(getCustomerAccountId(accountId.getDisplayName()));
        templateTable.addAttribute(getExtraAttr(EXTRA_ATTR_NAME, accountId.getDisplayName()));
        metadataProxy.createImportTable(mainCustomerSpace, TEMPLATE_NAME, templateTable);

        Map<String, String> renameMap = new HashedMap<>();
        renameMap.put(InterfaceName.AccountId.name(), InterfaceName.CustomerAccountId.name());
        Map<String, String> duplicateMap = new HashedMap<>();
        duplicateMap.put(InterfaceName.AccountId.name(), EXTRA_ATTR_NAME);
        ConvertBatchStoreToImportRequest request = new ConvertBatchStoreToImportRequest();
        request.setEntity(BusinessEntity.Account);
        request.setUserId("DEFAULT_CONVERTER");
        request.setFeedType(BusinessEntity.Account.name() + "Data");
        request.setRenameMap(renameMap);
        request.setDuplicateMap(duplicateMap);
        request.setTemplateName(TEMPLATE_NAME);

        ApplicationId appId = cdlProxy.convertBatchStoreToImport(mainCustomerSpace, request);
        log.info("ConvertBatchStoreToImport application id: " + appId.toString());
        com.latticeengines.domain.exposed.workflow.JobStatus completedStatus = waitForWorkflowStatus(appId.toString(),
                false);
        assertEquals(completedStatus, JobStatus.COMPLETED);
    }

    private Attribute getCustomerAccountId(String displayName) {
        Attribute attribute = new Attribute();
        attribute.setName(InterfaceName.CustomerAccountId.name());
        attribute.setDisplayName(displayName);
        attribute.setInterfaceName(InterfaceName.CustomerAccountId);
        attribute.setAllowedDisplayNames(Arrays.asList("ACCOUNT", "ACCOUNT_ID", "ACCOUNTID","ACCOUNT_EXTERNAL_ID"));
        attribute.setPhysicalDataType(Schema.Type.STRING.toString());
        attribute.setLogicalDataType(LogicalDataType.Id);
        attribute.setFundamentalType(FundamentalType.ALPHA);
        attribute.setApprovedUsage(ModelingMetadata.NONE_APPROVED_USAGE);
        return attribute;
    }

    private Attribute getExtraAttr(String attrName, String displayName) {
        Attribute attribute = new Attribute();
        attribute.setName(attrName);
        attribute.setDisplayName(displayName);
        attribute.setPhysicalDataType(Schema.Type.STRING.toString());
        attribute.setLogicalDataType(LogicalDataType.Id);
        attribute.setFundamentalType(FundamentalType.ALPHA);
        attribute.setApprovedUsage(ModelingMetadata.NONE_APPROVED_USAGE);
        return attribute;
    }

}
