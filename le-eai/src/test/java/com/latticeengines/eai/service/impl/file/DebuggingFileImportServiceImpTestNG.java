package com.latticeengines.eai.service.impl.file;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.File;
import java.io.FileInputStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
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
public class DebuggingFileImportServiceImpTestNG extends EaiFunctionalTestNGBase {

    @Inject
    private ImportService fileImportService;

    @Inject
    private Configuration yarnConfiguration;

    private URL metadataUrl;

    private URL dataUrl;

    private String fileName = "";

    @BeforeClass(groups = "manual")
    public void setup() throws Exception {
        cleanup();
    }

    public void cleanup() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, "/tmp/dataFromFile");
        HdfsUtils.rmdir(yarnConfiguration, "/tmp/sourceFiles");
        HdfsUtils.mkdir(yarnConfiguration, "/tmp/sourceFiles");
        // change file name
        dataUrl = ClassLoader.getSystemResource("com/latticeengines/eai/service/impl/file/" + fileName);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, dataUrl.getPath(), "/tmp/sourceFiles");
        // change metadata json
        metadataUrl = ClassLoader.getSystemResource("com/latticeengines/eai/service/impl/file/table.json");
    }

    @Test(groups = "manual", dataProvider = "getPropertiesProvider", enabled = true)
    public void importMetadataAndDataAndWriteToHdfs(Map<String, String> properties) throws Exception {
        cleanup();
        ImportContext ctx = new ImportContext(yarnConfiguration);
        ctx.setProperty(ImportProperty.TARGETPATH, "/tmp/dataFromFile/file2");
        ctx.setProperty(ImportProperty.CUSTOMER, "testcustomer");
        ctx.setProperty(ImportProperty.EXTRACT_PATH, new HashMap<String, String>());
        ctx.setProperty(ImportProperty.PROCESSED_RECORDS, new HashMap<String, Long>());
        ctx.setProperty(ImportProperty.LAST_MODIFIED_DATE, new HashMap<String, Long>());

        SourceImportConfiguration fileImportConfig = new SourceImportConfiguration();
        fileImportConfig.setSourceType(SourceType.FILE);
        Table t = JsonUtils.deserialize(
                IOUtils.toString(new FileInputStream(new File(metadataUrl.getPath())), Charset.defaultCharset()),
                Table.class);
        fileImportConfig.setTables(Arrays.<Table> asList(new Table[] { t }));
        fileImportConfig.setProperties(properties);

        List<Table> tables = Arrays.<Table> asList(new Table[] { t });

        fileImportConfig.setTables(Arrays.<Table> asList(tables.get(0)));
        fileImportService.importDataAndWriteToHdfs(fileImportConfig, ctx, null);

        ApplicationId appId = ctx.getProperty(ImportProperty.APPID, ApplicationId.class);
        assertNotNull(appId);
        FinalApplicationStatus status = platformTestBase.waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

    @DataProvider
    public Object[][] getPropertiesProvider() {
        Map<String, String> metadataFileProperties = getProperties(true);
        return new Object[][] { { metadataFileProperties } };
    }

    private Map<String, String> getProperties(boolean useMetadataFile) {
        Map<String, String> props = new HashMap<>();
        props.put(ImportProperty.HDFSFILE, "/tmp/sourceFiles/" + fileName);
        return props;
    }
}
