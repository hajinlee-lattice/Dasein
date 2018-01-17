package com.latticeengines.apps.cdl.end2end.dataingestion;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.Resource;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.workflow.JobStatus;

public class CleanupByUploadDeploymentTestNG extends DataIngestionEnd2EndDeploymentTestNGBase {


    @Test(groups = "end2end")
    public void runTest() throws Exception {
        resumeCheckpoint(ProcessTransactionDeploymentTestNG.CHECK_POINT);
        testBed.excludeTestTenantsForCleanup(Collections.singletonList(mainTestTenant));
        verify();
    }


    private void verify() {
        String customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();

        Table table = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.ConsolidatedContact);
        List<GenericRecord> recordsBeforeDelete = getRecords(customerSpace, table);
        String fieldName = table.getAttribute(InterfaceName.ContactId.name()).getName();
        StringBuilder sb = new StringBuilder();
        sb.append("id");
        sb.append(',');
        sb.append("index");
        sb.append('\n');
        int index = 0;
        for (GenericRecord record : recordsBeforeDelete) {
            sb.append(record.get(fieldName).toString());
            sb.append(',');
            sb.append(index);
            sb.append('\n');
            index++;
            if (index == 10) {
                break;
            }
        }
        assert (index > 0);
        String fileName = "contact_delete.csv";
        Resource source = new ByteArrayResource(sb.toString().getBytes()) {
            @Override
            public String getFilename() {
                return fileName;
            }
        };
        String appId = uploadDeleteCSV(fileName, SchemaInterpretation.DeleteContactTemplate,
                CleanupOperationType.BYUPLOAD_ID,
                source);
        JobStatus status = waitForWorkflowStatus(appId, false);
        assertEquals(status, JobStatus.COMPLETED);

        Table table2 = dataCollectionProxy.getTable(customerSpace, TableRoleInCollection.ConsolidatedContact);
        List<GenericRecord> recordsAfterDelete = getRecords(customerSpace, table2);
        assertEquals(recordsBeforeDelete.size(), recordsAfterDelete.size() + index);
    }

    private List<GenericRecord> getRecords(String customerSpace, Table table) {
        assertNotNull(table);
        List<Extract> extracts = table.getExtracts();
        assertNotNull(extracts);
        List<String> paths = new ArrayList<String>();
        for (Extract e : extracts) {
            paths.add(e.getPath());
        }
        return AvroUtils.getDataFromGlob(yarnConfiguration, paths);
    }
}
