package com.latticeengines.apps.cdl.end2end;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchVersion;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public class HardDeleteDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(HardDeleteDeploymentTestNG.class);

    private final String DeleteJoinId = "AccountId";

    @Inject
    private ActionProxy actionProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private MatchProxy matchProxy;

    private String customerSpace;

    private Set<String> idSets = new HashSet<>();

    @BeforeClass(groups = "end2end")
    @Override
    public void setup() throws Exception {
        log.info("Running setup with ENABLE_ENTITY_MATCH enabled!");
        Map<String, Boolean> featureFlagMap = new HashMap<>();
        featureFlagMap.put(LatticeFeatureFlag.ENABLE_ENTITY_MATCH.getName(), true);
        setupEnd2EndTestEnvironment(featureFlagMap);
        resumeCheckpoint(ProcessTransactionWithAdvancedMatchDeploymentTestNG.CHECK_POINT);
        log.info("Setup Complete!");
        customerSpace = CustomerSpace.parse(mainCustomerSpace).getTenantId();
    }

    @Test(groups = "end2end")
    public void runTest() throws Exception {
//        importData();
        registerDeleteData();
        EntityMatchVersion entityMatchVersion = matchProxy.getEntityMatchVersion(customerSpace,
                EntityMatchEnvironment.SERVING, false);
        log.info("entityMatchVersion is {}.", entityMatchVersion);
        ProcessAnalyzeRequest request = new ProcessAnalyzeRequest();
        request.setFullRematch(true);
        processAnalyze(request);
        EntityMatchVersion entityMatchVersionAfterPA = matchProxy.getEntityMatchVersion(customerSpace,
                EntityMatchEnvironment.SERVING, false);
        Assert.assertEquals(entityMatchVersion.getNextVersion(), entityMatchVersionAfterPA.getCurrentVersion());
        log.info("after PA, entityMatchVersion is {}.", entityMatchVersion);
        DataCollection.Version activeVersion = dataCollectionProxy.getActiveVersion(customerSpace);
        verifyServingStoreVersion(activeVersion.complement(), entityMatchVersion.getNextVersion());
        verifyHardDelete();
    }

    private void registerDeleteData() {
        Table table = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.ConsolidatedAccount);
        List<GenericRecord> recordsBeforeDelete = getRecords(table);
        int originalNumRecords = recordsBeforeDelete.size();
        log.info("There are " + originalNumRecords + " rows in avro before delete.");
        int numRecordsInCsv = 0;
        String fieldName = table.getAttribute(InterfaceName.AccountId.name()).getName();
        StringBuilder sb = new StringBuilder();
        sb.append(DeleteJoinId);
        sb.append(',');
        sb.append("index");
        sb.append('\n');
        for (GenericRecord record : recordsBeforeDelete) {
            String id = record.get(fieldName).toString();
            sb.append(id);
            sb.append(',');
            sb.append(numRecordsInCsv);
            sb.append('\n');
            idSets.add(id);
            numRecordsInCsv++;
            if (numRecordsInCsv == 10 || numRecordsInCsv == 20) {

                log.info("There are " + numRecordsInCsv + " rows in csv.");
                String fileName = "account_delete_" + numRecordsInCsv + ".csv";
                Resource source = new ByteArrayResource(sb.toString().getBytes()) {
                    @Override
                    public String getFilename() {
                        return fileName;
                    }
                };
                SourceFile sourceFile = uploadDeleteCSV(fileName, SchemaInterpretation.RegisterDeleteDataTemplate,
                        CleanupOperationType.BYUPLOAD_ID,
                        source);
                ApplicationId appId = cdlProxy.registerDeleteData(customerSpace, MultiTenantContext.getEmailAddress(),
                        sourceFile.getName(), true);
                JobStatus status = waitForWorkflowStatus(appId.toString(), false);
                Assert.assertEquals(JobStatus.COMPLETED, status);
                if (numRecordsInCsv == 20) {
                    break;
                }
                sb = new StringBuilder();
                sb.append(DeleteJoinId);
                sb.append(',');
                sb.append("index");
                sb.append('\n');
            }
        }
        assert (numRecordsInCsv > 0);
    }

    private List<GenericRecord> getRecords(Table table) {
        Assert.assertNotNull(table);
        List<Extract> extracts = table.getExtracts();
        Assert.assertNotNull(extracts);
        List<String> paths = new ArrayList<>();
        for (Extract extract : table.getExtracts()) {
            paths.add(PathUtils.toAvroGlob(extract.getPath()));
        }
        return AvroUtils.getDataFromGlob(yarnConfiguration, paths);
    }

    private void verifyHardDelete() {
        log.info("idSet is {}", idSets);
        verifyHardDeleteByRole(TableRoleInCollection.ConsolidatedAccount);
        verifyHardDeleteByRole(TableRoleInCollection.ConsolidatedContact);
        verifyHardDeleteByRole(TableRoleInCollection.ConsolidatedRawTransaction);
    }

    private void verifyHardDeleteByRole(TableRoleInCollection tableRoleInCollection) {
        Table table = dataCollectionProxy.getTable(customerSpace, tableRoleInCollection);
        List<GenericRecord> recordsAfterDelete = getRecords(table);
        int originalNumRecords = recordsAfterDelete.size();
        log.info("There are {} rows in avro after delete. table role is {}.", originalNumRecords, tableRoleInCollection);
        for (GenericRecord record : recordsAfterDelete) {
            String accountId = record.get(InterfaceName.AccountId.name()).toString();
            Assert.assertTrue(!idSets.contains(accountId));
        }
    }

    protected void importData() throws Exception {
        mockCSVImport(BusinessEntity.Contact, ADVANCED_MATCH_SUFFIX, 1, "DefaultSystem_ContactData");
        Thread.sleep(2000);
    }

    private void verifyServingStoreVersion(DataCollection.Version version, int currentVersion) {
        DataCollectionStatus dataCollectionStatus = dataCollectionProxy
                .getOrCreateDataCollectionStatus(mainTestTenant.getId(), version);
        Assert.assertEquals(dataCollectionStatus.getServingStoreVersion(), currentVersion);
    }

}
