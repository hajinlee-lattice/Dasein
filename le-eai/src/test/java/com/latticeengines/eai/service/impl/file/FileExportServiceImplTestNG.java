package com.latticeengines.eai.service.impl.file;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobID;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.ExportContext;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.ExportProperty;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.eai.file.runtime.mapreduce.CSVExportJob;
import com.latticeengines.eai.functionalframework.EaiMiniClusterFunctionalTestNGBase;
import com.latticeengines.eai.service.ExportService;
import com.latticeengines.eai.service.impl.file.strategy.CSVFileExportStrategyBase;

public class FileExportServiceImplTestNG extends EaiMiniClusterFunctionalTestNGBase {

    private static final CustomerSpace TEST_CUSTOMER = CustomerSpace.parse("TestCustomer");

    @Autowired
    private ExportService fileExportService;

    @Autowired
    private CSVFileExportStrategyBase csvFileExportStrategyBase;

    private URL dataUrl;

    private String sourceFilePath;

    private String targetCSVPath;

    private URL csvUrl;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        super.setup();
        dataUrl = ClassLoader.getSystemResource("com/latticeengines/eai/service/impl/file/file.avro");
        sourceFilePath = "/tmp/sourceFiles";
        targetCSVPath = "output";
        HdfsUtils.rmdir(miniclusterConfiguration, sourceFilePath);
        HdfsUtils.mkdir(miniclusterConfiguration, sourceFilePath);
        HdfsUtils.copyLocalToHdfs(miniclusterConfiguration, dataUrl.getPath(), sourceFilePath);

        csvUrl = ClassLoader.getSystemResource("com/latticeengines/eai/service/impl/file/file2.csv");
        HdfsUtils.copyLocalToHdfs(miniclusterConfiguration, csvUrl.getPath(), sourceFilePath);
    }

    @AfterClass(groups = "functional")
    public void cleanup() throws IOException {
        HdfsUtils.rmdir(miniclusterConfiguration, sourceFilePath);
        HdfsUtils.rmdir(miniclusterConfiguration,
                PathBuilder.buildDataFileExportPath(CamilleEnvironment.getPodId(), TEST_CUSTOMER).toString());
    }

    @Test(groups = "functional")
    public void exportMetadataAndDataAndWriteToHdfs() throws Exception {
        ExportContext ctx = new ExportContext(miniclusterConfiguration);

        ExportConfiguration fileExportConfig = new ExportConfiguration();
        fileExportConfig.setExportFormat(ExportFormat.CSV);
        fileExportConfig.setExportDestination(ExportDestination.FILE);
        fileExportConfig.setCustomerSpace(TEST_CUSTOMER);
        fileExportConfig
                .setExportTargetPath(PathBuilder.buildDataFileExportPath(CamilleEnvironment.getPodId(), TEST_CUSTOMER)
                        .append(targetCSVPath).toString());
        // fileExportConfig.setUsingDisplayName(Boolean.FALSE);
        Table table = createFile(new File(csvUrl.getPath()).getParentFile(), "file2");
        Extract extract = new Extract();
        extract.setPath(sourceFilePath + "/file.avro");
        table.setExtracts(Arrays.<Extract> asList(new Extract[] { extract }));

        fileExportConfig.setTable(table);
        Map<String, String> props = new HashMap<>();
        props.put(ExportProperty.TARGET_FILE_NAME, targetCSVPath);
        fileExportConfig.setProperties(props);

        ((FileExportServiceImpl) fileExportService).configureExportContext(fileExportConfig, ctx);
        JobID jobId = testMRJob(CSVExportJob.class, csvFileExportStrategyBase.getProperties(ctx, table));
        assertNotNull(jobId);

        String actualTargetPath = PathBuilder.buildDataFileExportPath(CamilleEnvironment.getPodId(), TEST_CUSTOMER)
                .append(targetCSVPath + "_file.csv").toString();
        verifyAllDataNotNullWithNumRows(actualTargetPath, 10);
    }

    void verifyAllDataNotNullWithNumRows(String csvPath, int num) throws IOException {
        try (FileSystem fs = FileSystem.newInstance(miniclusterConfiguration)) {
            try (InputStream is = fs.open(new Path(csvPath))) {

                try (InputStreamReader reader = new InputStreamReader(is)) {
                    CSVFormat format = LECSVFormat.format;
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
