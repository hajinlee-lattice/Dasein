package com.latticeengines.pls.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBaseDeprecated;
import com.latticeengines.pls.service.ModelingFileMetadataService;

public class ModelMetadataServiceImplTestNG extends PlsFunctionalTestNGBaseDeprecated {

    private InputStream fileInputStream;

    private File dataFile;

    @Autowired
    private ModelingFileMetadataService modelingFileMetadataService;

    @Test(groups = "functional")
    public void uploadFileWithMissingRequiredFields() throws Exception {
        dataFile = new File(ClassLoader.getSystemResource(
                "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file_missing_required_fields.csv").getPath());
        fileInputStream = new BufferedInputStream(new FileInputStream(dataFile));
        CloseableResourcePool closeableResourcePool = new CloseableResourcePool();
        try {
            modelingFileMetadataService.validateHeaderFields(fileInputStream, SchemaInterpretation.SalesforceAccount,
                    closeableResourcePool, dataFile.getName());
            closeableResourcePool.close();
        } catch (Exception e) {
            assertTrue(e instanceof LedpException);
            assertTrue(e.getMessage().contains(InterfaceName.Id.name()));
            assertTrue(e.getMessage().contains(InterfaceName.Website.name()));
            assertTrue(e.getMessage().contains(InterfaceName.Event.name()));
            assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_18087);
        } finally {
            closeableResourcePool.close();
        }
    }

    @Test(groups = "functional")
    public void uploadFileWithEmptyHeaderName() throws Exception {
        dataFile = new File(ClassLoader.getSystemResource(
                "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file_empty_header.csv").getPath());
        fileInputStream = new BufferedInputStream(new FileInputStream(dataFile));
        CloseableResourcePool closeableResourcePool = new CloseableResourcePool();
        try {
            modelingFileMetadataService.validateHeaderFields(fileInputStream, SchemaInterpretation.SalesforceAccount,
                    closeableResourcePool, dataFile.getName());
        } catch (Exception e) {
            assertTrue(e instanceof LedpException);
            assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_18096);
        } finally {
            closeableResourcePool.close();
        }
    }

    @Test(groups = "functional")
    public void uploadFileWithUnexpectedCharacterInHeaderName() throws Exception {
        dataFile = new File(ClassLoader.getSystemResource(
                "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file_unexpected_character_in_header.csv")
                .getPath());
        fileInputStream = new BufferedInputStream(new FileInputStream(dataFile));
        CloseableResourcePool closeableResourcePool = new CloseableResourcePool();
        try {
            modelingFileMetadataService.validateHeaderFields(fileInputStream, SchemaInterpretation.SalesforceAccount,
                    closeableResourcePool, dataFile.getName());
        } catch (Exception e) {
            assertTrue(e instanceof LedpException);
            assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_18095);
        } finally {
            closeableResourcePool.close();
        }
    }
}
