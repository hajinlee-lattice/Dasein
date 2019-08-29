package com.latticeengines.pls.end2end;


import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.joda.time.DateTime;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.EaiImportJobDetail;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.proxy.exposed.eai.EaiJobDetailProxy;

public class CSVFileImportValidationDeploymentTestNG extends CSVFileImportDeploymentTestNGBase {

    // one line with empty ID, two line with illegal char
    private static final String ACCOUNT_SOURCE_FILE = "Account_With_Invalid_Char.csv";

    private static final String CONTACT_SOURCE_FILE = "Contact_Insufficient_Info.csv";

    private static final String PRODUCT_HIERARCHY_SOURCE_FILE = "Product_Without_Family_File.csv";

    private static final String PRODUCT_BUNDLE_WITHOUT_NAME = "Product_Bundles_Without_NAME.csv";

    @Inject
    private EaiJobDetailProxy eaiJobDetailProxy;

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
        startCDLImport(accountFile, ENTITY_ACCOUNT);
        verifyAvroFileNumber(accountFile, 47, targetPath);
        getDataFeedTask(ENTITY_ACCOUNT);
        String accountIdentifier = accountDataFeedTask.getUniqueId();
        EaiImportJobDetail accountDetail = eaiJobDetailProxy
                .getImportJobDetailByCollectionIdentifier(accountIdentifier);
        verifyEaiJobDetail(accountDetail, 3L, 47);

        SourceFile contactFile = uploadSourceFile(CONTACT_SOURCE_FILE, ENTITY_CONTACT);
        String contactPath = String.format("%s/%s/DataFeed1/DataFeed1-Contact/Extracts",
                PathBuilder
                        .buildDataTablePath(CamilleEnvironment.getPodId(), CustomerSpace.parse(mainTestTenant.getId()))
                        .toString(),
                SourceType.FILE.getName());
        startCDLImport(contactFile, ENTITY_CONTACT);
        verifyAvroFileNumber(contactFile, 47, contactPath);
        getDataFeedTask(ENTITY_CONTACT);
        String contactIdentifier = contactDataFeedTask.getUniqueId();
        EaiImportJobDetail contactDetail = eaiJobDetailProxy
                .getImportJobDetailByCollectionIdentifier(contactIdentifier);
        verifyEaiJobDetail(contactDetail, 3L, 47);

        SourceFile productFile = uploadSourceFile(PRODUCT_HIERARCHY_SOURCE_FILE, ENTITY_PRODUCT);
        verifyFailed(productFile, ENTITY_PRODUCT);
        List<?> list = restTemplate.getForObject(getRestAPIHostPort() + "/pls/reports", List.class);
        List<Report> reports = JsonUtils.convertList(list, Report.class);
        Collections.sort(reports, Comparator.comparing(Report::getCreated));
        Assert.assertEquals(reports.size(), 3);
        Report accountReport = reports.get(0);
        Report contactReport = reports.get(1);
        Report productReport = reports.get(2);
        verifyReport(accountReport, 3L, 3L, 47L);
        verifyReport(contactReport, 3L, 3L, 47L);
        verifyReport(productReport, 0L, 2L, 0L);
    }

    @Test(groups = "deployment")
    public void testProductNameMissing() {
        SourceFile sourceFile = uploadSourceFile(PRODUCT_SOURCE_FILE, ENTITY_PRODUCT);
        startCDLImport(sourceFile, ENTITY_PRODUCT);
        // re-import the file without product name
        sourceFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_PRODUCT), ENTITY_PRODUCT, PRODUCT_BUNDLE_WITHOUT_NAME,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + PRODUCT_BUNDLE_WITHOUT_NAME));

        String feedType = getFeedTypeByEntity(DEFAULT_SYSTEM, ENTITY_PRODUCT);
        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(sourceFile.getName(), ENTITY_PRODUCT, SOURCE, feedType);
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getMappedField() == null) {
                fieldMapping.setMappedField(fieldMapping.getUserField());
                fieldMapping.setMappedToLatticeField(false);
            }
        }
        modelingFileMetadataService.resolveMetadata(sourceFile.getName(), fieldMappingDocument, ENTITY_PRODUCT, SOURCE,
                feedType);
        sourceFile = sourceFileService.findByName(sourceFile.getName());
        ApplicationId applicationId = cdlService.submitCSVImport(customerSpace, sourceFile.getName(),
                sourceFile.getName(), SOURCE, ENTITY_PRODUCT, getFeedTypeByEntity(DEFAULT_SYSTEM, ENTITY_PRODUCT));

        JobStatus completedStatus = waitForWorkflowStatus(workflowProxy, applicationId.toString(), false);
        Assert.assertEquals(completedStatus, JobStatus.FAILED);
    }

}
