package com.latticeengines.pls.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.ModelingFileMetadataService;

public class ModelingFileMetadataServiceImplTestNG extends PlsFunctionalTestNGBase {

    private InputStream fileInputStream;

    private File dataFile;

    @Value("${pls.modelingservice.basedir}")
    private String modelingBaseDir;

    @Autowired
    private ModelingFileMetadataService modelingFileMetadataService;

    @Override
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
    }

    @Test(groups = "functional")
    public void uploadFileWithMissingRequiredFields() throws Exception {
        dataFile = new File(ClassLoader.getSystemResource(
                "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file_missing_required_fields.csv").getPath());
        fileInputStream = new BufferedInputStream(new FileInputStream(dataFile));
        CloseableResourcePool closeableResourcePool = new CloseableResourcePool();
        boolean thrown = false;
        try {
            modelingFileMetadataService.validateHeaderFields(fileInputStream, closeableResourcePool,
                    dataFile.getName(), true);
            closeableResourcePool.close();
        } catch (Exception e) {
            thrown = true;
            assertTrue(e instanceof LedpException);
            assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_18120);
        } finally {
            closeableResourcePool.close();
            assertTrue(thrown);
        }
    }

    @Test(groups = "functional")
    public void uploadFileWithEmptyHeaderName() throws Exception {
        dataFile = new File(ClassLoader.getSystemResource(
                "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file_empty_header.csv").getPath());
        fileInputStream = new BufferedInputStream(new FileInputStream(dataFile));
        CloseableResourcePool closeableResourcePool = new CloseableResourcePool();
        boolean thrown = false;

        try {
            modelingFileMetadataService.validateHeaderFields(fileInputStream, closeableResourcePool,
                    dataFile.getName(), true);
        } catch (Exception e) {
            thrown = true;
            assertTrue(e instanceof LedpException);
            assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_18096);
        } finally {
            closeableResourcePool.close();
            assertTrue(thrown);
        }
    }

    @Test(groups = "functional")
    public void uploadFileWithUnexpectedCharacterInHeaderName() throws Exception {
        dataFile = new File(ClassLoader.getSystemResource(
                "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file_unexpected_character_in_header.csv")
                .getPath());
        fileInputStream = new BufferedInputStream(new FileInputStream(dataFile));
        CloseableResourcePool closeableResourcePool = new CloseableResourcePool();

        modelingFileMetadataService.validateHeaderFields(fileInputStream, closeableResourcePool, dataFile.getName(),
                true);

        closeableResourcePool.close();

    }

    @Test(groups = "functional")
    public void uploadFileWithDuplicateHeaders1() throws Exception {
        dataFile = new File(ClassLoader.getSystemResource(
                "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file_duplicate_headers1.csv").getPath());
        fileInputStream = new BufferedInputStream(new FileInputStream(dataFile));
        CloseableResourcePool closeableResourcePool = new CloseableResourcePool();
        boolean thrown = false;
        try {
            modelingFileMetadataService.validateHeaderFields(fileInputStream, closeableResourcePool,
                    dataFile.getName(), true);
        } catch (Exception e) {
            thrown = true;
        } finally {
            closeableResourcePool.close();
            assertTrue(thrown);
        }
    }

    @Test(groups = "functional")
    public void uploadFileWithHeadersHavingReservedWords() throws Exception {
        dataFile = new File(ClassLoader.getSystemResource(
                "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file_headers_with_reserved_words.csv").getPath());
        fileInputStream = new BufferedInputStream(new FileInputStream(dataFile));
        CloseableResourcePool closeableResourcePool = new CloseableResourcePool();
        boolean thrown = false;
        try {
            modelingFileMetadataService.validateHeaderFields(fileInputStream, closeableResourcePool,
                    dataFile.getName(), true);
        } catch (Exception e) {
            thrown = true;
            assertTrue(e instanceof LedpException);
            assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_18122);
        } finally {
            closeableResourcePool.close();
            assertTrue(thrown);
        }
    }

}
