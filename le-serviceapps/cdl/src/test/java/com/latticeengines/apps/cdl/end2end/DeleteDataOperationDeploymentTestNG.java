package com.latticeengines.apps.cdl.end2end;

import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.DataDeleteOperationConfiguration;
import com.latticeengines.domain.exposed.cdl.DataOperationRequest;
import com.latticeengines.domain.exposed.metadata.DataOperation;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.DeleteActionConfiguration;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

/**
 * Going to reuse this test for Soft delete, so the setup step looks redundant
 * for now.
 */
/*
 * dpltc deploy -a pls,admin,cdl,modeling,lp,metadata,workflowapi,eai
 */
public class DeleteDataOperationDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DeleteDataOperationDeploymentTestNG.class);

    @Inject
    private ActionProxy actionProxy;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    S3Service s3Service;

    private RetryTemplate retry;

    private String customerSpace;

    private Set<String> accountIdsForAll = new HashSet<>();

    private Set<String> contactIdsForAll = new HashSet<>();

    private List<Integer> expectedUploadSize = new ArrayList<>();

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
        retry = RetryUtils.getRetryTemplate(10, //
                Collections.singleton(AssertionError.class), null);
    }

    @Test(groups = "end2end")
    public void testRegisterDeleteData() throws Exception {
        extractIds();
        triggerDataOperations();
        verifyRegister();
        processAnalyze();
        verifyAfterPA();
    }

    private void extractIds() {
        String accountTableName = dataCollectionProxy.getTableName(customerSpace,
                TableRoleInCollection.ConsolidatedAccount);
        Table accountTable = metadataProxy.getTableSummary(customerSpace, accountTableName);

        Iterable<GenericRecord> accountItr = iterateRecords(accountTable);

        for (GenericRecord record : accountItr) {
            String accountId = record.get(InterfaceName.AccountId.name()).toString();
            accountIdsForAll.add(accountId);
            System.out.println("accountId:" + accountId);
            if (accountIdsForAll.size() >= 20) {
                break;
            }
        }
    }

    private void triggerDataOperations() throws Exception {
        triggerDataOperation("account_all.csv", accountIdsForAll, Account);
    }

    private void triggerDataOperation(String fileName, Collection<String> ids, BusinessEntity idEntity) throws Exception {
        if (CollectionUtils.isEmpty(ids)) {
            log.info("Only 0 rows in {}. Skip registering the delete action.", fileName);
            return;
        }
        DataDeleteOperationConfiguration configuration = new DataDeleteOperationConfiguration();
        configuration.setEntity(idEntity);
        configuration.setDeleteType(DataDeleteOperationConfiguration.DeleteType.SOFT);
        String dropPath = cdlProxy.createDataOperation(customerSpace, DataOperation.OperationType.DELETE,configuration);
        Assert.assertNotNull(dropPath);
        System.out.println(dropPath);

        retry.execute(context -> {
            DataOperation dataOperation = cdlProxy.findDataOperationByDropPath(customerSpace, dropPath);
            Assert.assertNotNull(dataOperation);
            Assert.assertEquals(dataOperation.getDropPath(), dropPath);
            Assert.assertEquals(((DataDeleteOperationConfiguration)dataOperation.getConfiguration()).getDeleteType(),
                    DataDeleteOperationConfiguration.DeleteType.SOFT);
            return true;
        });

        File tmpFile = generateCsv("AccountId", ids);
        s3Service.uploadLocalFile(s3Bucket, dropPath + fileName, tmpFile, true);

        DataOperationRequest dataOperationRequest = new DataOperationRequest();
        dataOperationRequest.setS3Bucket(s3Bucket);
        dataOperationRequest.setS3DropPath(dropPath);
        dataOperationRequest.setS3FileKey(dropPath + fileName);

        ApplicationId appId = cdlProxy.submitDataOperationJob(customerSpace, dataOperationRequest);
        JobStatus status = waitForWorkflowStatus(appId.toString(), false);
        Assert.assertEquals(JobStatus.COMPLETED, status);
        expectedUploadSize.add(ids.size());
    }

    private File generateCsv(String idField, Collection<String> ids) throws Exception {
        File tempFile = File.createTempFile("temp-", ".csv");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))) {
            writer.write(idField);
            writer.newLine();
            for (String id : ids) {
                writer.write(id);
                writer.newLine();
            }
        }
        return tempFile;
    }

    private void verifyRegister() {
        List<Action> softDeletes = actionProxy.getActions(customerSpace).stream()
                .filter(action -> ActionType.SOFT_DELETE.equals(action.getType())).collect(Collectors.toList());
        Assert.assertNotNull(softDeletes);
        Assert.assertEquals(softDeletes.size(), expectedUploadSize.size());
        log.info("Soft Delet Actions: " + JsonUtils.serialize(softDeletes));

        for (int i = 0; i < expectedUploadSize.size(); i++) {
            DeleteActionConfiguration deleteActionConfiguration = (DeleteActionConfiguration) softDeletes.get(i)
                    .getActionConfiguration();
            Assert.assertNotNull(deleteActionConfiguration);
            String tableName = deleteActionConfiguration.getDeleteDataTable();
            Table registeredDeleteTable = metadataProxy.getTable(customerSpace, tableName);
            Assert.assertNotNull(registeredDeleteTable);
            Integer count = (int) StreamSupport.stream(iterateRecords(registeredDeleteTable).spliterator(), false)
                    .count();
            Assert.assertEquals(count, expectedUploadSize.get(i));
        }
    }

    private void verifyAfterPA() {
        Set<String> accountIdsToDelete = new HashSet<>(accountIdsForAll);
        Table table = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.ConsolidatedAccount);
        for (GenericRecord record : iterateRecords(table)) {
            String accountId = record.get(InterfaceName.AccountId.name()).toString();
            Assert.assertFalse(accountIdsToDelete.contains(accountId), "Should not contain id " + accountId);
        }
        table = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.SystemAccount);
        for (GenericRecord record : iterateRecords(table)) {
            String accountId = record.get(InterfaceName.EntityId.name()).toString();
            Assert.assertFalse(accountIdsToDelete.contains(accountId), "Should not contain id " + accountId);
        }
    }

    private Iterable<GenericRecord> iterateRecords(Table table) {
        Assert.assertNotNull(table);
        List<Extract> extracts = table.getExtracts();
        Assert.assertNotNull(extracts);
        List<String> paths = new ArrayList<>();
        for (Extract e : extracts) {
            paths.add(PathUtils.toAvroGlob(e.getPath()));
        }
        return () -> AvroUtils.iterateAvroFiles(yarnConfiguration, paths);
    }

}
