package com.latticeengines.apps.cdl.end2end;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.JobStatus;

public class CleanupByUploadWithEntityMatchGADeploymentTestNG extends CDLEnd2EndDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(CleanupByUploadWithEntityMatchGADeploymentTestNG.class);

    private String customerSpace;

    private int numRecordsInCsv = 0;
    private int originalNumRecords;

    @BeforeClass(groups = "end2end")
    @Override
    public void setup() throws Exception {
        log.info("Running setup with ENABLE_ENTITY_MATCH_GA enabled!");
        Map<String, Boolean> featureFlagMap = new HashMap<>();
        featureFlagMap.put(LatticeFeatureFlag.ENABLE_ENTITY_MATCH_GA.getName(), true);
        setupEnd2EndTestEnvironment(featureFlagMap);
        resumeCheckpoint(ProcessAccountWithAdvancedMatchDeploymentTestNG.CHECK_POINT);
        log.info("Setup Complete!");
        customerSpace = CustomerSpace.parse(mainCustomerSpace).getTenantId();
    }

    @Test(groups = "end2end")
    public void runTest() {
        cleanupByUpload();
        verifyCleanup();
    }

    private void verifyCleanup() {
        Table table2 = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.ConsolidatedContact);
        List<GenericRecord> recordsAfterDelete = getRecords(table2);
        log.info("There are " + recordsAfterDelete.size() + " rows in avro after delete.");
        Assert.assertEquals(originalNumRecords, recordsAfterDelete.size() + numRecordsInCsv);
    }

    private void cleanupByUpload() {
        Table table = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.ConsolidatedContact);
        List<GenericRecord> recordsBeforeDelete = getRecords(table);
        originalNumRecords = recordsBeforeDelete.size();
        log.info("There are " + originalNumRecords + " rows in avro before delete.");
        String fieldName = table.getAttribute(InterfaceName.CustomerContactId.name()).getName();
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
