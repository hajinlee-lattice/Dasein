package com.latticeengines.eai.service.impl.file;

import static org.junit.Assert.assertNotNull;
import static org.testng.Assert.assertEquals;

import java.io.File;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.mapreduce.counters.RecordImportCounter;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.eai.file.runtime.mapreduce.CSVImportJob;
import com.latticeengines.eai.functionalframework.EaiMiniClusterFunctionalTestNGBase;
import com.latticeengines.eai.service.EaiMetadataService;
import com.latticeengines.eai.service.ImportService;
import com.latticeengines.eai.service.impl.file.strategy.FileEventTableImportStrategyBase;
import com.latticeengines.yarn.exposed.service.JobService;

public class FileImportServiceImplTestNG extends EaiMiniClusterFunctionalTestNGBase {

    @Autowired
    private ImportService fileImportService;

    @Autowired
    private EaiMetadataService eaiMetadataService;

    @Autowired
    private FileEventTableImportStrategyBase fileEventTableImportStrategyBase;

    private URL metadataUrl;

    private URL dataUrl;

    private URL avroDataUrl;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        super.setup();
        cleanup();
    }

    public void cleanup() throws Exception {
        HdfsUtils.rmdir(miniclusterConfiguration, "/tmp/dataFromFile");
        HdfsUtils.rmdir(miniclusterConfiguration, "/tmp/sourceFiles");
        HdfsUtils.mkdir(miniclusterConfiguration, "/tmp/sourceFiles");
        dataUrl = ClassLoader.getSystemResource("com/latticeengines/eai/service/impl/file/file2.csv");
        HdfsUtils.copyLocalToHdfs(miniclusterConfiguration, dataUrl.getPath(), "/tmp/sourceFiles");
        metadataUrl = ClassLoader.getSystemResource("com/latticeengines/eai/service/impl/file/testdataMetadata.json");
        avroDataUrl = ClassLoader.getSystemResource("com/latticeengines/eai/service/impl/file/file3.avro");
    }

    @Test(groups = "functional", dataProvider = "getPropertiesProvider")
    public void importMetadataAndDataAndWriteToHdfs(Map<String, String> properties) throws Exception {
        cleanup();
        ImportContext ctx = new ImportContext(miniclusterConfiguration);
        ctx.setProperty(ImportProperty.TARGETPATH, "/tmp/dataFromFile/file2");
        ctx.setProperty(ImportProperty.CUSTOMER, "testcustomer");
        ctx.setProperty(ImportProperty.EXTRACT_PATH, new HashMap<String, String>());
        ctx.setProperty(ImportProperty.PROCESSED_RECORDS, new HashMap<String, Long>());
        ctx.setProperty(ImportProperty.LAST_MODIFIED_DATE, new HashMap<String, Long>());

        SourceImportConfiguration fileImportConfig = new SourceImportConfiguration();
        fileImportConfig.setSourceType(SourceType.FILE);
        fileImportConfig
                .setTables(Arrays.<Table> asList(new Table[] { createFile(new File(avroDataUrl.getPath()), "file2") }));
        fileImportConfig.setProperties(properties);

        List<Table> tables = fileImportService.importMetadata(fileImportConfig, ctx, null);

        ctx.setProperty(ImportProperty.HDFSFILE, //
                properties.get(ImportProperty.HDFSFILE));
        ctx.setProperty(ImportProperty.FILEURLPROPERTIES, //
                properties.get(ImportProperty.FILEURLPROPERTIES));

        Job mrJob = createMRJob(CSVImportJob.class, fileEventTableImportStrategyBase.getProperties(ctx, tables.get(0)));
        JobID jobID = JobService.runMRJob(mrJob, "jobName", true);
        assertNotNull(jobID);
        fileEventTableImportStrategyBase.updateContextProperties(ctx, tables.get(0));
        eaiMetadataService.updateTableSchema(tables, ctx);

        verifyAllDataNotNullWithNumRows(miniclusterConfiguration, //
                tables.get(0), //
                8);
        Counters counters = mrJob.getCounters();
        assertEquals(counters.findCounter(RecordImportCounter.IMPORTED_RECORDS).getValue(), 8);
        assertEquals(counters.findCounter(RecordImportCounter.IGNORED_RECORDS).getValue(), 11);
        assertEquals(counters.findCounter(RecordImportCounter.REQUIRED_FIELD_MISSING).getValue(), 5);
        assertEquals(counters.findCounter(RecordImportCounter.FIELD_MALFORMED).getValue(), 6);
        assertEquals(counters.findCounter(RecordImportCounter.ROW_ERROR).getValue(), 0);
    }

    @DataProvider
    public Object[][] getPropertiesProvider() {
        Map<String, String> metadataFileProperties = getProperties(true);
        // Map<String, String> inlineMetadataProperties = getProperties(false);

        return new Object[][] { { metadataFileProperties } };
    }

    private Map<String, String> getProperties(boolean useMetadataFile) {
        Map<String, String> props = new HashMap<>();
        if (useMetadataFile) {
            props.put(ImportProperty.METADATAFILE, metadataUrl.getPath());
        } else {
            String contents;
            try {
                contents = FileUtils.readFileToString(new File(metadataUrl.getPath()), Charset.defaultCharset());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            props.put(ImportProperty.METADATA, contents);
        }
        props.put(ImportProperty.HDFSFILE, "/tmp/sourceFiles/file2.csv");
        return props;
    }

}
