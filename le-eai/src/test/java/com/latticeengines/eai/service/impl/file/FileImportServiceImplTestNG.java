package com.latticeengines.eai.service.impl.file;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.relique.jdbc.csv.CsvDriver;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;
import com.latticeengines.eai.service.ImportService;

public class FileImportServiceImplTestNG extends EaiFunctionalTestNGBase {

    @Autowired
    private ImportService fileImportService;

    @Autowired
    private Configuration yarnConfiguration;

    private URL metadataUrl;

    private URL dataUrl;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        cleanup();
    }

    public void cleanup() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, "/tmp/dataFromFile");
        HdfsUtils.rmdir(yarnConfiguration, "/tmp/sourceFiles");
        HdfsUtils.mkdir(yarnConfiguration, "/tmp/sourceFiles");
        dataUrl = ClassLoader.getSystemResource("com/latticeengines/eai/service/impl/file/file1.csv");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, dataUrl.getPath(), "/tmp/sourceFiles");
        metadataUrl = ClassLoader.getSystemResource("com/latticeengines/eai/service/impl/file/file1Metadata.json");
    }

    @Test(groups = "functional", dataProvider = "getPropertiesProvider")
    public void importMetadataAndDataAndWriteToHdfs(Map<String, String> properties) throws Exception {
        cleanup();
        ImportContext ctx = new ImportContext(yarnConfiguration);
        ctx.setProperty(ImportProperty.TARGETPATH, "/tmp/dataFromFile/file1");
        ctx.setProperty(ImportProperty.CUSTOMER, "testcustomer");

        SourceImportConfiguration fileImportConfig = new SourceImportConfiguration();
        fileImportConfig.setSourceType(SourceType.FILE);
        fileImportConfig.setTables(Arrays.<Table> asList(new Table[] { createFile(
                new File(dataUrl.getPath()).getParentFile(), "file1") }));
        fileImportConfig.setProperties(properties);

        List<Table> tables = fileImportService.importMetadata(fileImportConfig, ctx);
        fileImportConfig.setTables(Arrays.<Table> asList(tables.get(0)));
        fileImportService.importDataAndWriteToHdfs(fileImportConfig, ctx);

        ApplicationId appId = ctx.getProperty(ImportProperty.APPID, ApplicationId.class);
        assertNotNull(appId);
        FinalApplicationStatus status = platformTestBase.waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
        verifyAllDataNotNullWithNumRows(yarnConfiguration, //
                ctx.getProperty(ImportProperty.TARGETPATH, String.class), //
                4);
    }

    @DataProvider
    public Object[][] getPropertiesProvider() {
        Map<String, String> metadataFileProperties = getProperties(true);
        Map<String, String> inlineMetadataProperties = getProperties(false);

        return new Object[][] { { metadataFileProperties }, { inlineMetadataProperties } };
    }

    private Map<String, String> getProperties(boolean useMetadataFile) {
        Map<String, String> props = new HashMap<>();
        if (useMetadataFile) {
            props.put(ImportProperty.METADATAFILE, metadataUrl.getPath());
        } else {
            String contents;
            try {
                contents = FileUtils.readFileToString(new File(metadataUrl.getPath()));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            props.put(ImportProperty.METADATA, contents);
        }
        props.put(ImportProperty.HDFSFILE, "/tmp/sourceFiles/file1.csv");
        Map<String, String> urlProperties = new HashMap<>();
        urlProperties.put(CsvDriver.DATE_FORMAT, "MM-DD-YYYY");
        props.put(ImportProperty.FILEURLPROPERTIES, JsonUtils.serialize(urlProperties));
        return props;
    }

}
