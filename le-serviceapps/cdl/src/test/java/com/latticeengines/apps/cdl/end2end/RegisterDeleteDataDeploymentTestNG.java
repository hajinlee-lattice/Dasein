package com.latticeengines.apps.cdl.end2end;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.DeleteActionConfiguration;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

/**
 * Going to reuse this test for Soft delete, so the setup step looks redundant for now.
 */
/*
 * dpltc deploy -a pls,admin,cdl,modeling,lp,metadata,workflowapi,eai
 */
public class RegisterDeleteDataDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(RegisterDeleteDataDeploymentTestNG.class);

    @Inject
    private ActionProxy actionProxy;

    @Inject
    private MetadataProxy metadataProxy;

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
    public void testRegisterDeleteData() {
        registerDeleteData();
        verifyRegister();
        processAnalyze();
        verifyAfterPA();
    }

    private void registerDeleteData() {
        Table table = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.ConsolidatedAccount);
        List<GenericRecord> recordsBeforeDelete = getRecords(table);
        int originalNumRecords = recordsBeforeDelete.size();
        log.info("There are " + originalNumRecords + " rows in avro before delete.");
        int numRecordsInCsv = 0;
        String fieldName = table.getAttribute(InterfaceName.AccountId.name()).getName();
        StringBuilder sb = new StringBuilder();
        sb.append("id");
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
            if (numRecordsInCsv == 10) {
                break;
            }
        }
        assert (numRecordsInCsv > 0);
        log.info("There are " + numRecordsInCsv + " rows in csv.");
        String fileName = "account_delete.csv";
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
                sourceFile.getName(), false);
        JobStatus status = waitForWorkflowStatus(appId.toString(), false);
        Assert.assertEquals(JobStatus.COMPLETED, status);
    }

    private void verifyRegister() {
        List<Action> softDelete = actionProxy.getActions(customerSpace).stream()
                .filter(action -> ActionType.SOFT_DELETE.equals(action.getType()))
                .collect(Collectors.toList());
        Assert.assertNotNull(softDelete);
        Assert.assertEquals(softDelete.size(), 1);
        Assert.assertNotNull(softDelete.get(0).getActionConfiguration());
        DeleteActionConfiguration deleteActionConfiguration = (DeleteActionConfiguration) softDelete.get(0).getActionConfiguration();
        String tableName = deleteActionConfiguration.getDeleteDataTable();
        Table registeredDeleteTable = metadataProxy.getTable(customerSpace, tableName);
        Assert.assertNotNull(registeredDeleteTable);

        List<GenericRecord> allDeletedRecords = getRecords(registeredDeleteTable);
        Assert.assertEquals(allDeletedRecords.size(), idSets.size());
        for (GenericRecord record : allDeletedRecords) {
            String accountId = record.get(InterfaceName.AccountId.name()).toString();
            Assert.assertTrue(idSets.contains(accountId));
        }
    }

    private void verifyAfterPA() {
        log.info("Ids that needs to be removed: " + idSets.toString());
        Table table = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.ConsolidatedAccount);
        List<GenericRecord> recordsAfterDelete = getRecords(table);
        for (GenericRecord record : recordsAfterDelete) {
            String accountId = record.get(InterfaceName.AccountId.name()).toString();
            Assert.assertFalse(idSets.contains(accountId), "Should not contain id " + accountId);
        }
    }

    private List<GenericRecord> getRecords(Table table) {
        Assert.assertNotNull(table);
        List<Extract> extracts = table.getExtracts();
        Assert.assertNotNull(extracts);
        List<String> paths = new ArrayList<>();
        for (Extract e : extracts) {
            paths.add(e.getPath());
        }
        return AvroUtils.getDataFromGlob(yarnConfiguration, paths);
    }

}
