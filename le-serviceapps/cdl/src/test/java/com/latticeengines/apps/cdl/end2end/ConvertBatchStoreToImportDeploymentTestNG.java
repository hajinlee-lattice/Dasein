package com.latticeengines.apps.cdl.end2end;

import static org.testng.Assert.assertEquals;

import java.util.Arrays;
import java.util.Map;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.commons.collections4.map.HashedMap;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.cdl.ConvertBatchStoreToImportRequest;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public class ConvertBatchStoreToImportDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ConvertBatchStoreToImportDeploymentTestNG.class);

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    private static final String TEMPLATE_NAME = "ConvertedAccountTemplate";
    private static final String EXTRA_ATTR_NAME = "Extra_Duplicate_Id";

    @BeforeClass(groups = { "end2end" })
    public void setup() throws Exception {
//        Map<String, Boolean> featureFlagMap = new HashMap<>();
//        featureFlagMap.put(LatticeFeatureFlag.ENABLE_ENTITY_MATCH_GA.getName(), true);
//        setupEnd2EndTestEnvironment(featureFlagMap);
        setupEnd2EndTestEnvironment();
        resumeCheckpoint(ProcessTransactionDeploymentTestNG.CHECK_POINT);
    }

    @Test(groups = "end2end")
    public void runTest() {
        convertAccountBatchStore();
        verifyConvert();
    }

    private void verifyConvert() {
        //
        System.out.println("done!");
    }

    private void convertAccountBatchStore() {
        Table batchStore = dataCollectionProxy.getTable(mainCustomerSpace, BusinessEntity.Account.getBatchStore());
        Table templateTable = TableUtils.clone(batchStore, TEMPLATE_NAME);
        Assert.assertNotNull(templateTable.getAttribute(InterfaceName.AccountId));
        Assert.assertNull(templateTable.getAttribute(InterfaceName.CustomerAccountId));
        Attribute accountId = templateTable.getAttribute(InterfaceName.AccountId);
        templateTable.removeAttribute(InterfaceName.AccountId.name());
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
