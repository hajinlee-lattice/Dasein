package com.latticeengines.apps.cdl.end2end;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;

public abstract class DeleteActivityStoreDeploymentTestNG extends ProcessActivityStoreDeploymentTestNG {

    private static final Logger log = LoggerFactory.getLogger(DeleteActivityStoreDeploymentTestNG.class);

    protected String customerSpace;

    protected Set<String> idSets = new HashSet<>();

    @Inject
    private CDLProxy cdlProxy;

    @BeforeClass(groups = {"end2end"})
    @Override
    public void setup() throws Exception {
        super.setup();
        customerSpace = CustomerSpace.parse(mainCustomerSpace).getTenantId();
    }

    protected void registerDeleteData(boolean hardDelete) throws IOException {
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
        int deleteSize = recordsBeforeDelete.size() / 2;
        for (GenericRecord record : recordsBeforeDelete) {
            String id = record.get(fieldName).toString();
            sb.append(id);
            sb.append(',');
            sb.append(numRecordsInCsv);
            sb.append('\n');
            idSets.add(id);
            numRecordsInCsv++;
            if (numRecordsInCsv >= deleteSize) {
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
                sourceFile.getName(), hardDelete);
        JobStatus status = waitForWorkflowStatus(appId.toString(), false);
        Assert.assertEquals(JobStatus.COMPLETED, status);
    }

    protected List<GenericRecord> getRecords(Table table) {
        Assert.assertNotNull(table);
        List<Extract> extracts = table.getExtracts();
        Assert.assertNotNull(extracts);
        List<String> paths = new ArrayList<>();
        for (Extract e : extracts) {
            paths.add(e.getPath());
        }
        return AvroUtils.getDataFromGlob(yarnConfiguration, paths);
    }

    protected List<GenericRecord> getStreamRecords(Table table) throws IOException {
        Assert.assertNotNull(table);
        List<Extract> extracts = table.getExtracts();
        Assert.assertNotNull(extracts);
        List<String> paths = new ArrayList<>();
        for (Extract e : extracts) {
            List<String> allFiles = HdfsUtils.getFilesForDir(yarnConfiguration, e.getPath());
            allFiles = allFiles.stream().filter(file -> {
                try {
                    return HdfsUtils.isDirectory(yarnConfiguration, file);
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
                return false;
            }).map(s -> s = s + "/*.avro").collect(Collectors.toList());
            paths.addAll(allFiles);

        }
        return AvroUtils.getDataFromGlob(yarnConfiguration, paths);
    }
}
