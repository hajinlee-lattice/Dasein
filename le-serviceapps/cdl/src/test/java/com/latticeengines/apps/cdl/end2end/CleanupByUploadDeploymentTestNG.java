package com.latticeengines.apps.cdl.end2end;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.JobStatus;

public class CleanupByUploadDeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(CleanupByUploadDeploymentTestNG.class);
    private static final int ACCOUNT_IMPORT_SIZE_1 = 500;
    private static final int CONTACT_IMPORT_SIZE_1 = 1100;
    private static final int PRODUCT_IMPORT_SIZE_1 = 100;
    private static final int TRANSACTION_IMPORT_SIZE_1 = 30000;
    private String customerSpace;

    int numRecordsInCsv = 0;
    int originalNumRecords;

    @Test(groups = "end2end")
    public void testDeleteContactByUpload() throws Exception {
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        resumeCheckpoint(ProcessTransactionDeploymentTestNG.CHECK_POINT);
        cleanupByUpload();
        verifyCleanup();
        processAnalyze();
        // verifyProcess();
    }

    private void cleanupByUpload() {
        Table table = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.ConsolidatedContact);
        List<GenericRecord> recordsBeforeDelete = getRecords(table);
        originalNumRecords = recordsBeforeDelete.size();
        log.info("There are " + originalNumRecords + " rows in avro before delete.");
        String fieldName = table.getAttribute(InterfaceName.ContactId.name()).getName();
        StringBuilder sb = new StringBuilder();
        sb.append("id");
        sb.append(',');
        sb.append("index");
        sb.append('\n');
        for (GenericRecord record : recordsBeforeDelete) {
            sb.append(record.get(fieldName).toString());
            sb.append(',');
            sb.append(numRecordsInCsv);
            sb.append('\n');
            numRecordsInCsv++;
            if (numRecordsInCsv == 10) {
                break;
            }
        }
        assert (numRecordsInCsv > 0);
        log.info("There are " + numRecordsInCsv + " rows in csv.");
        String fileName = "contact_delete.csv";
        Resource source = new ByteArrayResource(sb.toString().getBytes()) {
            @Override
            public String getFilename() {
                return fileName;
            }
        };
        SourceFile sourceFile = uploadDeleteCSV(fileName, SchemaInterpretation.DeleteContactTemplate,
                CleanupOperationType.BYUPLOAD_ID,
                source);
        ApplicationId appId = cdlProxy.cleanupByUpload(customerSpace, sourceFile,
                BusinessEntity.Contact, CleanupOperationType.BYUPLOAD_ID, MultiTenantContext.getEmailAddress());
        JobStatus status = waitForWorkflowStatus(appId.toString(), false);
        Assert.assertEquals(JobStatus.COMPLETED, status);
    }

    private void verifyCleanup() {
        Table table2 = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.ConsolidatedContact);
        List<GenericRecord> recordsAfterDelete = getRecords(table2);
        log.info("There are " + recordsAfterDelete.size() + " rows in avro after delete.");
        Assert.assertEquals(originalNumRecords, recordsAfterDelete.size() + numRecordsInCsv);
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



    private void verifyProcess() {
        runCommonPAVerifications();
        verifyStats(false, BusinessEntity.Account, BusinessEntity.Contact, BusinessEntity.PurchaseHistory);

        long numAccounts = ACCOUNT_IMPORT_SIZE_1;
        long numContacts = CONTACT_IMPORT_SIZE_1;
        long numProducts = PRODUCT_IMPORT_SIZE_1;
        long numTransactions = TRANSACTION_IMPORT_SIZE_1;

        System.out.println(countTableRole(BusinessEntity.Account.getBatchStore()));
        System.out.println(countTableRole(BusinessEntity.Contact.getBatchStore()));
        System.out.println(countTableRole(BusinessEntity.Product.getBatchStore()));
        System.out.println(countTableRole(TableRoleInCollection.ConsolidatedRawTransaction));
        System.out.println(countInRedshift(BusinessEntity.Account));
        System.out.println(countInRedshift(BusinessEntity.Contact));

//        Assert.assertEquals(countTableRole(BusinessEntity.Account.getBatchStore()), numAccounts);
//        Assert.assertEquals(countTableRole(BusinessEntity.Contact.getBatchStore()), numContacts);
//        Assert.assertEquals(countTableRole(BusinessEntity.Product.getBatchStore()), numProducts);
//        Assert.assertEquals(countTableRole(TableRoleInCollection.ConsolidatedRawTransaction), numTransactions);
//
//        Assert.assertEquals(countInRedshift(BusinessEntity.Account), numAccounts);
//        Assert.assertEquals(countInRedshift(BusinessEntity.Contact), numContacts);

        createTestSegment1();
        Map<BusinessEntity, Long> segment1Counts = ImmutableMap.of( //
                BusinessEntity.Account, SEGMENT_1_ACCOUNT_1,
                BusinessEntity.Contact, SEGMENT_1_CONTACT_1,
                BusinessEntity.Product, (long) PRODUCT_IMPORT_SIZE_1);
        //verifyTestSegment1Counts(segment1Counts);
        createTestSegment2();
        Map<BusinessEntity, Long> segment2Counts = ImmutableMap.of( //
                BusinessEntity.Account, SEGMENT_2_ACCOUNT_1,
                BusinessEntity.Contact, SEGMENT_2_CONTACT_1,
                BusinessEntity.Product, (long) PRODUCT_IMPORT_SIZE_1);
        //verifyTestSegment2Counts(segment2Counts);
        RatingEngine ratingEngine = createRuleBasedRatingEngine();
        Map<RatingBucketName, Long> ratingCounts = ImmutableMap.of( //
                RatingBucketName.A, RATING_A_COUNT_1, //
                RatingBucketName.D, RATING_D_COUNT_1, //
                RatingBucketName.F, RATING_F_COUNT_1
        );
        //verifyRatingEngineCount(ratingEngine.getId(), ratingCounts);
    }
}
