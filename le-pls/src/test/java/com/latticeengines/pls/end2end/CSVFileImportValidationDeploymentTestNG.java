package com.latticeengines.pls.end2end;

import static org.testng.Assert.assertEquals;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.workflow.JobStatus;

public class CSVFileImportValidationDeploymentTestNG extends CSVFileImportDeploymentTestNGBase {

    private static final String ACCOUNT_SOURCE_FILE = "Account_With_Invalid_Char.csv";
    private SourceFile accountFile;

    private static final String CONTACT_SOURCE_FILE = "Contact_Insufficient_Info.csv";
    private SourceFile contactFile;

    private static final String PRODUCT_HIERARCHY_SOURCE_FILE = "Product_Without_Family_File.csv";
    private SourceFile productFile;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG);
        MultiTenantContext.setTenant(mainTestTenant);
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
    }

    @Test(groups = "deployment")
    public void testInvalidFile() {
        accountFile = uploadSourceFile(ACCOUNT_SOURCE_FILE, ENTITY_ACCOUNT);
        startCDLImportAndVerify(accountFile, ENTITY_ACCOUNT);
        contactFile = uploadSourceFile(CONTACT_SOURCE_FILE, ENTITY_CONTACT);
        startCDLImportAndVerify(contactFile, ENTITY_CONTACT);
        productFile = uploadSourceFile(PRODUCT_HIERARCHY_SOURCE_FILE, ENTITY_PRODUCT);
        startCDLImportAndVerify(productFile, ENTITY_PRODUCT);

    }

    private void startCDLImportAndVerify(SourceFile sourceFile, String entity) {
        ApplicationId applicationId = cdlService.submitCSVImport(customerSpace, sourceFile.getName(),
                sourceFile.getName(), SOURCE, entity, getFeedTypeByEntity(DEFAULT_SYSTEM, entity));

        JobStatus completedStatus = waitForWorkflowStatus(workflowProxy, applicationId.toString(), false);
        assertEquals(completedStatus, JobStatus.FAILED);
    }
}
