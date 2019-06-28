package com.latticeengines.pls.end2end;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.pls.FileProperty;
import com.latticeengines.domain.exposed.pls.S3ImportTemplateDisplay;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;

public class CSVFileImportToS3DeploymentTestNG extends CSVFileImportDeploymentTestNGBase  {

    private static final Logger log = LoggerFactory.getLogger(CSVFileImportToS3DeploymentTestNG.class);

    @Inject
    private DropBoxProxy dropBoxProxy;
    @Inject
    private S3Service s3Service;

    @Value("${aws.customer.s3.bucket}")
    private String s3Bucket;

    private List<S3ImportTemplateDisplay> templates = null;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG);
        MultiTenantContext.setTenant(mainTestTenant);
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        templates = cdlService.getS3ImportTemplate(customerSpace, "");
        log.info("templates is :" + JsonUtils.serialize(templates));
    }

    @Test(groups = "deployment")
    public void testMain() {
        for (S3ImportTemplateDisplay display : templates) {
            importFile(display.getEntity().name(), display.getPath());
        }
        for (S3ImportTemplateDisplay display : templates) {
            List<FileProperty> fileLists = dropBoxProxy.getFileListForPath(customerSpace, display.getPath(), "csv");
            log.info("under the path: " + display.getPath() + " , the fileLists is " + JsonUtils.serialize(fileLists));
            switch (display.getEntity().name()) {
                case ENTITY_ACCOUNT:
                    Assert.assertEquals(fileLists.size(), 4);
                    testConfigTemplate(fileLists.get(0), display.getEntity().name(), display.getFeedType());
                    break;
                case ENTITY_CONTACT: Assert.assertEquals(fileLists.size(), 1);break;
                case ENTITY_TRANSACTION: Assert.assertEquals(fileLists.size(), 1);
                default:break;

            }
        }
    }

    private void importFile(String entity, String s3Path) {
        String key = PathUtils.formatKey(s3Bucket, s3Path);
        switch (entity) {
            case ENTITY_ACCOUNT: String path = key + "/" + ACCOUNT_SOURCE_FILE;
                s3Service.uploadInputStream(s3Bucket, path,
                        ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + ACCOUNT_SOURCE_FILE), true);
                path = key + "/" + ACCOUNT_SOURCE_FILE_FROMATDATE;
                s3Service.uploadInputStream(s3Bucket, path,
                        ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + ACCOUNT_SOURCE_FILE_FROMATDATE), true);
                path = key + "/" + ACCOUNT_SOURCE_FILE_MISSING;
                s3Service.uploadInputStream(s3Bucket, path,
                        ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + ACCOUNT_SOURCE_FILE_MISSING),
                        true);
                s3Service.createFolder(s3Bucket, key + "/subFolder1");
                break;
            case ENTITY_CONTACT:
                key = key + "/" + CONTACT_SOURCE_FILE;
                s3Service.uploadInputStream(s3Bucket, key,
                        ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + CONTACT_SOURCE_FILE),
                        true);
                break;
            case ENTITY_TRANSACTION:
                key = key + "/" + TRANSACTION_SOURCE_FILE;
                s3Service.uploadInputStream(s3Bucket, key,
                        ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + TRANSACTION_SOURCE_FILE),
                        true);
                break;
            default:break;
        }
    }

    private void testConfigTemplate(FileProperty csvFile, String entity, String feedType) {
        String uri = "/pls/models/uploadfile/importFile?entity=%s";
        uri = String.format(uri, entity);
        ResponseDocument responseDocument = restTemplate.postForObject(getRestAPIHostPort() + uri,
                csvFile, ResponseDocument.class);
        Assert.assertNotNull(responseDocument);
        Assert.assertTrue(responseDocument.isSuccess());
        SourceFile sourceFile = JsonUtils.convertValue(responseDocument.getResult(), SourceFile.class);
        Assert.assertEquals(sourceFile.getDisplayName(), csvFile.getFileName());
        uri = "/pls/models/uploadfile/%s/fieldmappings?entity=%s&source=%s&feedType=%s";
        uri = String.format(uri, sourceFile.getName(), entity, SourceType.FILE.getName(), feedType);
        responseDocument = restTemplate.postForObject(getRestAPIHostPort() + uri, null,
                ResponseDocument.class);
        Assert.assertNotNull(responseDocument);
        Assert.assertTrue(responseDocument.isSuccess());
        FieldMappingDocument fieldMappingDocument = JsonUtils.convertValue(responseDocument.getResult(),
                FieldMappingDocument.class);
        uri = "/pls/models/uploadfile/fieldmappings?displayName=%s&entity=%s&source=%s&feedType=%s";
        uri = String.format(uri, sourceFile.getName(), entity, SourceType.FILE.getName(), feedType);
        restTemplate.postForObject(getRestAPIHostPort() + uri, fieldMappingDocument,
                Void.class);
        uri = "/pls/cdl/import/csv?templateFileName=%s&dataFileName=%s&entity=%s&source=%s&feedType=%s";
        uri = String.format(uri, sourceFile.getName(), sourceFile.getName(), entity, SourceType.FILE.getName(),
                feedType);
        try {
            restTemplate.postForObject(getRestAPIHostPort() + uri, null, ResponseDocument.class);
        } catch (RuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("can not auto import"));
        }
    }
}
