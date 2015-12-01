package com.latticeengines.dataplatform.service.impl;

import static org.testng.Assert.assertEquals;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFilenameFilter;
import com.latticeengines.dataplatform.exposed.service.SqoopSyncJobService;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

public class SqoopSyncJobServiceImplTestNG extends DataPlatformFunctionalTestNGBase {

    private static final Log log = LogFactory.getLog(SqoopSyncJobServiceImplTestNG.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private SqoopSyncJobService sqoopSyncJobService;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, "/tmp/dataFromFile");
        HdfsUtils.rmdir(yarnConfiguration, "/tmp/dataFromDB");
    }

    @AfterClass(groups = "functional")
    public void tearDown() throws Exception {
        Collection<File> files = FileUtils.listFiles(new File("."), new IOFileFilter() {

            @Override
            public boolean accept(File file) {
                String name = file.getName();
                return name.contains("sqoop-import-props") && name.endsWith(".properties");
            }

            @Override
            public boolean accept(File dir, String name) {
                return false;
            }

        }, null);
        for (File file : files) {
            FileUtils.deleteQuietly(file);
        }
    }

    @Test(groups = "functional", enabled = true)
    public void importDataForFile() throws Exception {
        URL inputUrl = ClassLoader.getSystemResource("com/latticeengines/dataplatform/service/impl/sqoopSyncJobServiceImpl");
        String url = String.format("jdbc:relique:csv:%s", inputUrl.getPath());
        String driver = "org.relique.jdbc.csv.CsvDriver";
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.jdbcUrl(url).driverClass(driver);
        DbCreds creds = new DbCreds(builder);
        
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, inputUrl.getPath() + "/Nutanix.csv", "/tmp");

        String[] types = new String[] {
                "Long", //
                "String", //
                "String", //
                "Long", //
                "Long", //
                "Long", //
                "String", //
                "String", //
                "String", //
                "String", //
                "Long", //
                "String", //
                "Float", //
                "String", //
                "String", //
                "String", //
                "Float", //
                "Float",//
                "Float", //
                "Float", //
                "Float", //
                "String", //
                "Float", //
                "String", //
                "Float", //
                "String", //
                "String", //
                "String", //
                "String", //
                "String", //
                "Float", //
                "Float", //
                "Float", //
                "Float",//
                "Float", //
                "Float", //
                "String", //
                "Float", //
                "Float", //
                "String", //
                "String", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "String", //
                "String", //
                "String", //
                "String", //
                "String", //
                "String", //
                "Float", //
                "Float", //
                "String", //
                "String", //
                "String", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float", //
                "Float"
        };
        Properties props = new Properties();
        props.put("trimHeaders", "true");
        props.put("columnTypes", StringUtils.join(types, ","));
        props.put("yarn.mr.hdfs.resources", "/tmp/Nutanix.csv#Nutanix.csv");
        ApplicationId appId = sqoopSyncJobService.importData("Nutanix", //
                "/tmp/dataFromFile", //
                creds, //
                LedpQueueAssigner.getModelingQueueNameForSubmission(), //
                "Nutanix", //
                Arrays.<String>asList(new String[] { "Nutanix_EventTable_Clean" }), //
                null, //
                1, //
                props);

        log.info(String.format("Waiting for appId %s", appId));
        FinalApplicationStatus status = waitForStatus(appId, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);

        List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, "/tmp/dataFromFile", new HdfsFilenameFilter() {

            @Override
            public boolean accept(String fileName) {
                return fileName.endsWith(".avro");
            }

        });

        assertEquals(files.size(), 1);

        Schema schema = AvroUtils.getSchema(yarnConfiguration, new Path(files.get(0)));

        int i = 0;
        for (Field field : schema.getFields()) {
            Type type = field.schema().getTypes().get(0).getType();

            switch (type) {
            case DOUBLE:
                assertEquals(types[i], "Float");
                break;
            case LONG:
                assertEquals(types[i], "Long");
                break;
            case STRING:
                assertEquals(types[i], "String");
                break;
            default:
                break;
            }

            i++;
        }
    }
}


