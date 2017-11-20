package com.latticeengines.pls.end2end;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.pls.functionalframework.CDLDeploymentTestNGBase;
import com.latticeengines.pls.service.CDLImportService;
import com.latticeengines.pls.service.FileUploadService;
import com.latticeengines.pls.service.ModelingFileMetadataService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.pls.util.ValidateFileHeaderUtils;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class CSVFileImportDeploymentTestNG extends CDLDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(CSVFileImportDeploymentTestNG.class);

    private static final String SOURCE_FILE_LOCAL_PATH = "com/latticeengines/pls/service/impl/";
    private static final String SOURCE = "File";
    private static final String FEED_TYPE_SUFFIX = "Schema";

    private static final String ENTITY_ACCOUNT = "Account";
    private static final String ENTITY_CONTACT = "Contact";
    private static final String ENTITY_TRANSACTION = "Transaction";

    private static final String ACCOUNT_SOURCE_FILE = "Q_CDLAccount.csv";
    private static final String CONTACT_SOURCE_FILE = "";
    private static final String TRANSACTION_SOURCE_FILE = "";

    @Autowired
    private ModelingFileMetadataService modelingFileMetadataService;

    @Autowired
    private FileUploadService fileUploadService;

    @Autowired
    private SourceFileService sourceFileService;

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private WorkflowProxy workflowProxy;

    @Autowired
    private CDLImportService cdlImportService;


    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG);
        MultiTenantContext.setTenant(mainTestTenant);
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();

        uploadSourceFile(ACCOUNT_SOURCE_FILE, ENTITY_ACCOUNT);
//        uploadSourceFile(CONTACT_SOURCE_FILE, ENTITY_CONTACT);
//        uploadSourceFile(TRANSACTION_SOURCE_FILE, ENTITY_TRANSACTION);
    }

    private void uploadSourceFile(String sourceFileName, String entity) {
        SourceFile sourceFile = fileUploadService.uploadFile(sourceFileName, sourceFileName,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + sourceFileName));

        String feedType = entity + FEED_TYPE_SUFFIX;
        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService.getFieldMappingDocumentBestEffort
                (sourceFileName, entity, SOURCE, feedType);
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getMappedField() == null) {
                fieldMapping.setMappedField(fieldMapping.getUserField());
                fieldMapping.setMappedToLatticeField(false);
            }
        }
        modelingFileMetadataService.resolveMetadata(sourceFileName, fieldMappingDocument, entity, SOURCE, feedType);

        sourceFile = sourceFileService.findByName(sourceFile.getName());
        Table table = metadataProxy.getTable(customerSpace, sourceFile.getTableName());
        Set<String> headers = getHeaderFields(ClassLoader.getSystemResource(SOURCE_FILE_LOCAL_PATH + sourceFileName));
        compare(table, headers);

        ApplicationId applicationId = cdlImportService.submitCSVImport(customerSpace, sourceFileName, sourceFileName,
                SOURCE, entity, feedType);

        JobStatus completedStatus = waitForWorkflowStatus(workflowProxy, applicationId.toString(), false);
        assertEquals(completedStatus, JobStatus.COMPLETED);
    }

    @Test(groups = "deployment", enabled = false)
    public void verify() {

    }

    private Set<String> getHeaderFields(URL sourceFileURL) {
        try {
            CloseableResourcePool closeableResourcePool = new CloseableResourcePool();
            InputStream stream = new FileInputStream(new File(sourceFileURL.getFile()));

            return  ValidateFileHeaderUtils.getCSVHeaderFields(stream, closeableResourcePool);
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_00002, e);
        }
    }

    private void compare(Table table, Set<String> headers) {
        List<Attribute> attributes = table.getAttributes();
        assertEquals(attributes.size(), headers.size());

        for (Attribute attribute: attributes) {
            assertTrue(headers.contains(attribute.getDisplayName()));
        }
    }
}