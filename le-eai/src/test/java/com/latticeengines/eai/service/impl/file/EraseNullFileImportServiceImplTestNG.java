package com.latticeengines.eai.service.impl.file;

import java.io.File;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobID;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.mapreduce.counters.RecordImportCounter;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.eai.file.runtime.mapreduce.CSVImportJob;
import com.latticeengines.eai.functionalframework.EaiMiniClusterFunctionalTestNGBase;
import com.latticeengines.eai.service.EaiMetadataService;
import com.latticeengines.eai.service.ImportService;
import com.latticeengines.eai.service.impl.file.strategy.FileEventTableImportStrategyBase;
import com.latticeengines.yarn.exposed.service.JobService;

public class EraseNullFileImportServiceImplTestNG extends EaiMiniClusterFunctionalTestNGBase {

    @Inject
    private ImportService fileImportService;

    @Inject
    private EaiMetadataService eaiMetadataService;

    @Inject
    private FileEventTableImportStrategyBase fileEventTableImportStrategyBase;

    private URL metadataUrl;

    private URL dataUrl;

    private URL avroDataUrl;

    private String nullFileName = "fileNull";

    private final String ERASE_PREFIX = "Erase_";

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        super.setup();
        cleanup(nullFileName);
    }

    public void cleanup(String fileName) throws Exception {
        HdfsUtils.rmdir(miniclusterConfiguration, "/tmp/dataFromFile");
        HdfsUtils.rmdir(miniclusterConfiguration, "/tmp/sourceFiles");
        HdfsUtils.mkdir(miniclusterConfiguration, "/tmp/sourceFiles");
        dataUrl = ClassLoader.getSystemResource("com/latticeengines/eai/service/impl/file/" + fileName + ".csv");
        HdfsUtils.copyLocalToHdfs(miniclusterConfiguration, dataUrl.getPath(), "/tmp/sourceFiles");
        metadataUrl = ClassLoader.getSystemResource("com/latticeengines/eai/service/impl/file/testdataMetadata.json");
        avroDataUrl = ClassLoader.getSystemResource("com/latticeengines/eai/service/impl/file/file3.avro");
    }

    @Test(groups = "manual", dataProvider = "getPropertiesProvider")
    public void importNullDataAndWriteToHdfs(Map<String, String> properties) throws Exception {
        ImportContext ctx = new ImportContext(miniclusterConfiguration);
        ctx.setProperty(ImportProperty.TARGETPATH, "/tmp/dataFromFile/" + nullFileName);
        ctx.setProperty(ImportProperty.CUSTOMER, "testcustomer");
        ctx.setProperty(ImportProperty.EXTRACT_PATH, new HashMap<String, String>());
        ctx.setProperty(ImportProperty.PROCESSED_RECORDS, new HashMap<String, Long>());
        ctx.setProperty(ImportProperty.LAST_MODIFIED_DATE, new HashMap<String, Long>());
        ctx.setProperty(ImportProperty.ENABLE_ERASE_BY_NULL, "true");

        SourceImportConfiguration fileImportConfig = new SourceImportConfiguration();
        fileImportConfig.setSourceType(SourceType.FILE);
        fileImportConfig.setTables(
                Arrays.<Table> asList(new Table[] { createFile(new File(avroDataUrl.getPath()), nullFileName) }));
        fileImportConfig.setProperties(properties);

        List<Table> tables = fileImportService.importMetadata(fileImportConfig, ctx, null);

        ctx.setProperty(ImportProperty.HDFSFILE, //
                properties.get(ImportProperty.HDFSFILE));
        ctx.setProperty(ImportProperty.FILEURLPROPERTIES, //
                properties.get(ImportProperty.FILEURLPROPERTIES));

        Job mrJob = createMRJob(CSVImportJob.class, fileEventTableImportStrategyBase.getProperties(ctx, tables.get(0)));
        JobID jobID = JobService.runMRJob(mrJob, "jobName", true);
        Assert.assertNotNull(jobID);
        fileEventTableImportStrategyBase.updateContextProperties(ctx, tables.get(0));
        eaiMetadataService.updateTableSchema(tables, ctx);

        verifyNullRows(miniclusterConfiguration, tables.get(0));
        Counters counters = mrJob.getCounters();
        Assert.assertEquals(counters.findCounter(RecordImportCounter.IMPORTED_RECORDS).getValue(), 7);
        Assert.assertEquals(counters.findCounter(RecordImportCounter.IGNORED_RECORDS).getValue(), 12);
        Assert.assertEquals(counters.findCounter(RecordImportCounter.REQUIRED_FIELD_MISSING).getValue(), 5);
        Assert.assertEquals(counters.findCounter(RecordImportCounter.FIELD_MALFORMED).getValue(), 6);
        Assert.assertEquals(counters.findCounter(RecordImportCounter.ROW_ERROR).getValue(), 0);
    }

    private void verifyNullRows(Configuration config, Table table) throws Exception {
        List<Extract> extracts = table.getExtracts();
        String nullId = "9821131";
        for (Extract extract : extracts) {
            List<String> avroFiles = HdfsUtils.getFilesByGlob(config, extract.getPath());
            for (String avroFile : avroFiles) {
                try (FileReader<GenericRecord> reader = AvroUtils.getAvroFileReader(config,
                        new org.apache.hadoop.fs.Path(avroFile))) {
                    while (reader.hasNext()) {
                        GenericRecord record = reader.next();
                        String id = record.get(InterfaceName.Id.name()).toString();
                        if (nullId.equals(id)) {
                            Assert.assertTrue(Boolean.parseBoolean(
                                    record.get(ERASE_PREFIX + InterfaceName.CompanyName.name()).toString()));
                            Assert.assertNull(record.get(InterfaceName.CompanyName.name()));
                        } else {
                            Assert.assertNull(record.get(ERASE_PREFIX + InterfaceName.CompanyName.name()));
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
        props.put(ImportProperty.HDFSFILE, "/tmp/sourceFiles/" + nullFileName + ".csv");
        return props;
    }

}
