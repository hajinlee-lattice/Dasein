package com.latticeengines.eai.service.impl.file;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.File;
import java.io.FileInputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataplatform.exposed.service.JobService;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.mapreduce.counters.Counters;
import com.latticeengines.domain.exposed.mapreduce.counters.RecordImportCounter;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;
import com.latticeengines.eai.service.EaiMetadataService;
import com.latticeengines.eai.service.ImportService;

public class DebuggingFileImportServiceImpTestNG extends EaiFunctionalTestNGBase {

    @Autowired
    private ImportService fileImportService;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private EaiMetadataService eaiMetadataService;

    private URL metadataUrl;

    private URL dataUrl;

    @Autowired
    private JobService jobService;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        cleanup();
    }

    public void cleanup() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, "/tmp/dataFromFile");
        HdfsUtils.rmdir(yarnConfiguration, "/tmp/sourceFiles");
        HdfsUtils.mkdir(yarnConfiguration, "/tmp/sourceFiles");
        // change file name
        dataUrl = ClassLoader.getSystemResource("com/latticeengines/eai/service/impl/file/Lead.csv");
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, dataUrl.getPath(), "/tmp/sourceFiles");
        // change metadata json
        metadataUrl = ClassLoader.getSystemResource("com/latticeengines/eai/service/impl/file/testdataMetadata.json");
    }

    @Test(groups = "functional", dataProvider = "getPropertiesProvider", enabled = false)
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
        Table t = JsonUtils.deserialize(IOUtils.toString(new FileInputStream(new File(metadataUrl.getPath()))), Table.class);
        fileImportConfig.setTables(Arrays.<Table> asList(new Table[] { t}));
        fileImportConfig.setProperties(properties);

        List<Table> tables = Arrays.<Table> asList(new Table[] { t});

        fileImportConfig.setTables(Arrays.<Table> asList(tables.get(0)));
        fileImportService.importDataAndWriteToHdfs(fileImportConfig, ctx);

        ApplicationId appId = ctx.getProperty(ImportProperty.APPID, ApplicationId.class);
        assertNotNull(appId);
        FinalApplicationStatus status = platformTestBase.waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
        verifyAllDataNotNullWithNumRows(yarnConfiguration, //
                tables.get(0), //
                9);
        Counters counters = jobService.getMRJobCounters(appId.toString());
        assertEquals(counters.getCounter(com.latticeengines.domain.exposed.mapreduce.counters.RecordImportCounter.IMPORTED_RECORDS).getValue(), 9);
        assertEquals(counters.getCounter(RecordImportCounter.IGNORED_RECORDS).getValue(), 10);
        assertEquals(counters.getCounter(RecordImportCounter.REQUIRED_FIELD_MISSING).getValue(), 5);
        assertEquals(counters.getCounter(RecordImportCounter.FIELD_MALFORMED).getValue(), 5);
        assertEquals(counters.getCounter(RecordImportCounter.ROW_ERROR).getValue(), 0);
    }

    @DataProvider
    public Object[][] getPropertiesProvider() {
        Map<String, String> metadataFileProperties = getProperties(true);
        return new Object[][] { { metadataFileProperties }};
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
     // change file name
        props.put(ImportProperty.HDFSFILE, "/tmp/sourceFiles/Lead.csv");
        return props;
    }
}
