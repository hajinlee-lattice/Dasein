package com.latticeengines.eai.service.impl.file;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.exposed.service.JobService;
import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.ExportContext;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;
import com.latticeengines.eai.service.ExportService;

public class FileExportServiceImplTestNG extends EaiFunctionalTestNGBase {

    @Autowired
    private ExportService fileExportService;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private JobService jobService;

    private URL dataUrl;

    private String sourceAvroPath;

    private String targetCSVPath;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        dataUrl = ClassLoader.getSystemResource("com/latticeengines/eai/service/impl/file/file.avro");
        sourceAvroPath = "/tmp/sourceFiles/file.avro";
        targetCSVPath = "/tmp/csv";
        HdfsUtils.rmdir(yarnConfiguration, sourceAvroPath);
        HdfsUtils.rmdir(yarnConfiguration, targetCSVPath);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, dataUrl.getPath(), sourceAvroPath);

    }

    @AfterClass(groups = "functional")
    public void cleanup() throws IOException {
        HdfsUtils.rmdir(yarnConfiguration, sourceAvroPath);
        // HdfsUtils.rmdir(yarnConfiguration, targetCSVPath);
    }

    @Test(groups = "functional", dataProvider = "getPropertiesProvider")
    public void exportMetadataAndDataAndWriteToHdfs(Map<String, String> properties) throws Exception {
        ExportContext ctx = new ExportContext(yarnConfiguration);
        ctx.setProperty(ExportProperty.TARGETPATH, targetCSVPath);
        ctx.setProperty(ExportProperty.CUSTOMER, "testcustomer");

        ExportConfiguration fileExportConfig = new ExportConfiguration();
        fileExportConfig.setExportFormat(ExportFormat.CSV);
        fileExportConfig.setExportDestination(ExportDestination.FILE);
        List<Table> tables = Arrays.<Table> asList(new Table[] { new Table() });
        fileExportConfig.setTables(tables);
        fileExportConfig.setProperties(properties);

        fileExportConfig.setTables(Arrays.<Table> asList(tables.get(0)));
        fileExportService.exportDataFromHdfs(fileExportConfig, ctx);

        ApplicationId appId = ctx.getProperty(ExportProperty.APPID, ApplicationId.class);
        assertNotNull(appId);
        FinalApplicationStatus status = platformTestBase.waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
        verifyAllDataNotNullWithNumRows(yarnConfiguration, targetCSVPath + "/output.csv", 10);
    }

    void verifyAllDataNotNullWithNumRows(Configuration yarnConfiguration, String csvPath, int num) throws IOException {
        try (FileSystem fs = FileSystem.newInstance(yarnConfiguration)) {
            try (InputStream is = fs.open(new Path(csvPath))) {

                try (InputStreamReader reader = new InputStreamReader(is)) {
                    CSVFormat format = CSVFormat.RFC4180.withHeader().withDelimiter(',');
                    try (CSVParser parser = new CSVParser(reader, format)) {
                        assertEquals(parser.getRecords().size(), num);
                        for (CSVRecord record : parser.getRecords()) {
                            for (Iterator<String> iter = record.iterator(); iter.hasNext();) {
                                assertNotNull(iter.next());
                            }
                        }
                    }
                }

            }
        }
    }

    @DataProvider
    public Object[][] getPropertiesProvider() {
        Map<String, String> metadataFileProperties = getProperties(true);
        return new Object[][] { { metadataFileProperties } };
    }

    private Map<String, String> getProperties(boolean useMetadataFile) {
        Map<String, String> props = new HashMap<>();
        props.put(ExportProperty.HDFSFILE, sourceAvroPath);
        return props;
    }

}
