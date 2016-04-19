package com.latticeengines.pls.service.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;

import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBaseDeprecated;
import com.latticeengines.pls.service.ScoringFileMetadataService;

public class ScoringFileMetadataServiceImplTestNG extends PlsFunctionalTestNGBaseDeprecated {

    private static final String PATH = "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file1.csv";
    private static final String[] extraColumns = new String[] { "Id", "Website", "Event", "ExtraColumn" };
    private static final String[] sufficientColumns = new String[] { "Id", "Website", "Event" };
    private File csvFile;
    private CloseableResourcePool leCsvParser;
    private String displayName;

    @Autowired
    private ScoringFileMetadataService scoringFileMetadataService;

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        URL csvFileUrl = ClassLoader.getSystemResource(PATH);
        csvFile = new File(csvFileUrl.getFile());
        leCsvParser = new CloseableResourcePool();
        displayName = "file1.csv";
    }

    @Test(groups = "functional")
    public void testValidateHeaderFieldsWithSufficientColumns() throws FileNotFoundException {
        boolean noException = false;
        InputStream stream = new FileInputStream(csvFile);
        try {
            scoringFileMetadataService.validateHeaderFields(stream, Arrays.asList(sufficientColumns), leCsvParser,
                    displayName);
            noException = true;
        } catch (Exception e) {
            Assert.fail("Should not throw exception");
        }
        Assert.assertTrue(noException);
    }

    @Test(groups = "functional")
    public void testValidateHeaderFieldsWithExtraColumns() throws FileNotFoundException {
        boolean exception = false;
        InputStream stream = new FileInputStream(csvFile);
        try {
            scoringFileMetadataService.validateHeaderFields(stream, Arrays.asList(extraColumns), leCsvParser,
                    displayName);
            exception = true;
        } catch (Exception e) {
            Assert.assertFalse(exception);
        }
    }

}
