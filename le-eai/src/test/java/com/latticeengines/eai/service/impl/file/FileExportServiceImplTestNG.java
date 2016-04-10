package com.latticeengines.eai.service.impl.file;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
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
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.ExportContext;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.eai.functionalframework.EaiFunctionalTestNGBase;
import com.latticeengines.eai.service.ExportService;

public class FileExportServiceImplTestNG extends EaiFunctionalTestNGBase {

    public static final CustomerSpace TEST_CUSTOMER = CustomerSpace.parse("TestCustomer");
    
    @Autowired
    private ExportService fileExportService;

    @Autowired
    private Configuration yarnConfiguration;

    private URL dataUrl;

    private String sourceAvroPath;

    private String targetCSVPath;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        dataUrl = ClassLoader.getSystemResource("com/latticeengines/eai/service/impl/file/file.avro");
        sourceAvroPath = "/tmp/sourceFiles/file.avro";
        targetCSVPath = "output.csv";
        HdfsUtils.rmdir(yarnConfiguration, sourceAvroPath);
        HdfsUtils.rmdir(yarnConfiguration, targetCSVPath);
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, dataUrl.getPath(), sourceAvroPath);
    }

    @AfterClass(groups = "functional")
    public void cleanup() throws IOException {
        HdfsUtils.rmdir(yarnConfiguration, sourceAvroPath);
    }

    @Test(groups = "functional")
    public void exportMetadataAndDataAndWriteToHdfs() throws Exception {
        ExportContext ctx = new ExportContext(yarnConfiguration);

        ExportConfiguration fileExportConfig = new ExportConfiguration();
        fileExportConfig.setExportFormat(ExportFormat.CSV);
        fileExportConfig.setExportDestination(ExportDestination.FILE);
        fileExportConfig.setTable(MetadataConverter.getTable(yarnConfiguration, sourceAvroPath));
        fileExportConfig.setCustomerSpace(TEST_CUSTOMER);
        Map<String, String> props = new HashMap<>();
        props.put(ExportProperty.TARGETPATH, targetCSVPath);
        fileExportConfig.setProperties(props);
        fileExportService.exportDataFromHdfs(fileExportConfig, ctx);

        ApplicationId appId = ctx.getProperty(ExportProperty.APPID, ApplicationId.class);
        assertNotNull(appId);
        FinalApplicationStatus status = platformTestBase.waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
        String actualTargetPath = PathBuilder.buildDataFileExportPath(CamilleEnvironment.getPodId(), TEST_CUSTOMER)
                .append(targetCSVPath).toString();
        verifyAllDataNotNullWithNumRows(yarnConfiguration, actualTargetPath, 10);
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
}
