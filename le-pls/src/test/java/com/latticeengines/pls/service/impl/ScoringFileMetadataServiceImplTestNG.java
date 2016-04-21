package com.latticeengines.pls.service.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URL;

import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBaseDeprecated;
import com.latticeengines.pls.service.ScoringFileMetadataService;

public class ScoringFileMetadataServiceImplTestNG extends PlsFunctionalTestNGBaseDeprecated {

    private static final String PATH = "com/latticeengines/pls/service/impl/fileuploadserviceimpl/file1.csv";
    private File csvFile;
    private CloseableResourcePool closeableResourcePool;
    private String displayName;

    @Autowired
    private ScoringFileMetadataService scoringFileMetadataService;

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        URL csvFileUrl = ClassLoader.getSystemResource(PATH);
        csvFile = new File(csvFileUrl.getFile());
        closeableResourcePool = new CloseableResourcePool();
        displayName = "file1.csv";
    }

    @Test(groups = "functional")
    public void testValidateHeaderFieldsWithSufficientColumns() throws FileNotFoundException {
        boolean noException = false;
        InputStream stream = new FileInputStream(csvFile);
        try {
            scoringFileMetadataService.validateHeaderFields(stream, createTableWithSufficientColumns().getAttributes(),
                    closeableResourcePool, displayName);
            noException = true;
        } catch (Exception e) {
            Assert.fail("Should not throw exception");
            e.printStackTrace();
        }
        Assert.assertTrue(noException);
    }

    @Test(groups = "functional")
    public void testValidateHeaderFieldsWithExtraColumns() throws FileNotFoundException {
        boolean exception = false;
        InputStream stream = new FileInputStream(csvFile);
        try {
            scoringFileMetadataService.validateHeaderFields(stream, createTableWithExtraColumns().getAttributes(),
                    closeableResourcePool, displayName);
            exception = true;
        } catch (Exception e) {
            Assert.assertFalse(exception);
        }
    }

    private Table createTableWithSufficientColumns() {
        Table table = new Table();
        table.setName("TableWithSufficientColumns");

        Attribute att = new Attribute();
        att.setName(InterfaceName.Id.name());
        att.setDisplayName("Account ID");
        table.addAttribute(att);

        att = new Attribute();
        att.setName(InterfaceName.Website.name());
        att.setDisplayName("Website");
        table.addAttribute(att);

        att = new Attribute();
        att.setName(InterfaceName.Event.name());
        att.setDisplayName("Event");
        table.addAttribute(att);

        return table;
    }

    private Table createTableWithExtraColumns() {
        Table table = createTableWithSufficientColumns();
        table.setName("TableWithSufficientColumns");

        Attribute att = new Attribute();
        att.setName("ExtraColumn");
        att.setDisplayName("ExtraColumn");
        table.addAttribute(att);
        return table;
    }

}
