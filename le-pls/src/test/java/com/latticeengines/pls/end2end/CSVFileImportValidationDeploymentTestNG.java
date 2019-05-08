package com.latticeengines.pls.end2end;

import static org.testng.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.workflow.JobStatus;

public class CSVFileImportValidationDeploymentTestNG extends CSVFileImportDeploymentTestNGBase {

    private static final String ACCOUNT_SOURCE_FILE = "Account_With_Invalid_Char.csv";

    private static final String CONTACT_SOURCE_FILE = "Contact_Insufficient_Info.csv";

    private static final String PRODUCT_HIERARCHY_SOURCE_FILE = "Product_Without_Family_File.csv";

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG);
        MultiTenantContext.setTenant(mainTestTenant);
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
    }

    @Test(groups = "deployment")
    public void testInvalidFile() throws IOException {
        SourceFile accountFile = uploadSourceFile(ACCOUNT_SOURCE_FILE, ENTITY_ACCOUNT);
        String targetPath = String.format("%s/%s/DataFeed1/DataFeed1-Account/Extracts",
                PathBuilder
                        .buildDataTablePath(CamilleEnvironment.getPodId(), CustomerSpace.parse(mainTestTenant.getId()))
                        .toString(),
                SourceType.FILE.getName());
        verifyAccountOrContact(accountFile, ENTITY_ACCOUNT, 48, targetPath);
        SourceFile contactFile = uploadSourceFile(CONTACT_SOURCE_FILE, ENTITY_CONTACT);
        String contactPath = String.format("%s/%s/DataFeed1/DataFeed1-Contact/Extracts",
                PathBuilder
                        .buildDataTablePath(CamilleEnvironment.getPodId(), CustomerSpace.parse(mainTestTenant.getId()))
                        .toString(),
                SourceType.FILE.getName());
        verifyAccountOrContact(contactFile, ENTITY_CONTACT, 49, contactPath);
        SourceFile productFile = uploadSourceFile(PRODUCT_HIERARCHY_SOURCE_FILE, ENTITY_PRODUCT);
        verifyProduct(productFile, ENTITY_PRODUCT);

    }

    private void verifyAccountOrContact(SourceFile sourceFile, String entity, int num, String path) throws IOException {
        ApplicationId applicationId = cdlService.submitCSVImport(customerSpace, sourceFile.getName(),
                sourceFile.getName(), SOURCE, entity, getFeedTypeByEntity(DEFAULT_SYSTEM, entity));
        JobStatus completedStatus = waitForWorkflowStatus(workflowProxy, applicationId.toString(), false);
        assertEquals(completedStatus, JobStatus.COMPLETED);
        String avroFileName = sourceFile.getName().substring(0, sourceFile.getName().lastIndexOf("."));
        List<String> avroFiles = HdfsUtils.getFilesForDirRecursive(yarnConfiguration, path,
                file -> !file.isDirectory() && file.getPath().toString().contains(avroFileName)
                        && file.getPath().getName().endsWith("avro"));
        Assert.assertEquals(avroFiles.size(), 1);
        String avroFilePath = avroFiles.get(0).substring(0, avroFiles.get(0).lastIndexOf("/"));
        long rowCount = AvroUtils.count(yarnConfiguration, avroFilePath + "/*.avro");

        Assert.assertEquals(rowCount, num);
    }
    private void verifyProduct(SourceFile sourceFile, String entity) {
        ApplicationId applicationId = cdlService.submitCSVImport(customerSpace, sourceFile.getName(),
                sourceFile.getName(), SOURCE, entity, getFeedTypeByEntity(DEFAULT_SYSTEM, entity));

        JobStatus completedStatus = waitForWorkflowStatus(workflowProxy, applicationId.toString(), false);
        assertEquals(completedStatus, JobStatus.FAILED);
    }
}
