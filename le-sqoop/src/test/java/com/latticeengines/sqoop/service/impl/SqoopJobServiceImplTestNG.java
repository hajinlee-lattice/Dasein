package com.latticeengines.sqoop.service.impl;

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
import org.apache.commons.lang3.StringUtils;
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
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFilenameFilter;
import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;
import com.latticeengines.domain.exposed.dataplatform.SqoopImporter;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.sqoop.functionalframework.SqoopFunctionalTestNGBase;
import com.latticeengines.sqoop.exposed.service.SqoopJobService;

public class SqoopJobServiceImplTestNG extends SqoopFunctionalTestNGBase {

    private static final Log log = LogFactory.getLog(SqoopJobServiceImplTestNG.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private SqoopJobService sqoopJobService;

    private String targetJdbcHost = "10.51.15.145";

    private String targetJdbcPort = "1433";

    private String targetJdbcDb = "DELL_EBI_STAGE_FINAL_USE_DEV";

    private String targetJdbcType = "SQLServer";

    private String targetJdbcUser = "hadoop";

    private String targetJdbcPassword = CipherUtils.decrypt("8xuq8yYNOoNtHQpFel/J7w==");

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, "/tmp/sqoopFile");
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

    @Test(groups = "functional", enabled = false)
    public void importDataForFile() throws Exception {
        URL inputUrl = ClassLoader
                .getSystemResource("com/latticeengines/sqoop/service/impl/files");
        String url = String.format("jdbc:relique:csv:%s", inputUrl.getPath());
        String driver = "org.relique.jdbc.csv.CsvDriver";
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.jdbcUrl(url).driverClass(driver);
        DbCreds creds = new DbCreds(builder);

        HdfsUtils.copyLocalToHdfs(yarnConfiguration, inputUrl.getPath() + "/Nutanix.csv", "/tmp");

        String[] types = new String[] { "Long", //
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
                "Float", //
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
                "Float", //
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
                "Float" };
        Properties props = new Properties();
        props.put("trimHeaders", "true");
        props.put("columnTypes", StringUtils.join(types, ","));
        props.put("yarn.mr.hdfs.resources", "/tmp/Nutanix.csv#Nutanix.csv");

        SqoopImporter importer = new SqoopImporter.Builder().setTable("Nutanix").setTargetDir("/tmp/dataFromFile")
                .setDbCreds(creds).setQueue(LedpQueueAssigner.getModelingQueueNameForSubmission())
                .setCustomer("Nutanix").setSplitColumn("Nutanix_EventTable_Clean").setNumMappers(1)
                .setProperties(props).setSync(false).build();

        ApplicationId appId = sqoopJobService.importData(importer);

        log.info(String.format("Waiting for appId %s", appId));
        FinalApplicationStatus status = YarnUtils.waitFinalStatusForAppId(yarnConfiguration,
                appId, 600);
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

    @Test(groups = "functional", enabled = true)
    public void exportDataToSQLServerWithEnclosure() throws Exception {
        URL inputUrl = ClassLoader
                .getSystemResource("com/latticeengines/sqoop/service/impl/files");
        String targetTable = "STG_WARRANTY_GLOBAL";
        String sourceDir = "/tmp/Warranty_Dell.txt";
        List<String> targetColumns = Arrays.asList("ORDER_BUSINESS_UNIT_ID", "LOCAL_CHANNEL", "SERVICE_TAG_ID",
                "ORDER_NUMBER", "SERVICE_CONTRACT_ORDER_NUMBER", "SERVICE_CONTRACT_START_DATE",
                "SERVICE_CONTRACT_END_DATE", "SERVICE_LEVEL_DESC,WARRANTY_ITEM_NUM", "BRAND_DESC",
                "SERVICE_LEVEL_CODE", "CUSTOMER_NUMBER", "SERVICE_CONTRACT_CUSTOMER_NUMBER", "PRODUCT_LINE_PARENT",
                "PRODUCT_LINE_DESC", "PRODUCT_LINE", "SERVICE_CONTRACT_STATUS_DESC", "SOURCE_SYSTEM_UPDATE_DATE",
                "REGION_CODE");
        String optionalEnclosurePara = "--optionally-enclosed-by";
        String optionalEnclosureValue = "\\\"";
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, inputUrl.getPath() + "/Warranty_Dell.txt", "/tmp");

        SqoopExporter exporter = new SqoopExporter.Builder().setTable(targetTable).setSourceDir(sourceDir)
                .setDbCreds(getSQLServerCreds()).setQueue(LedpQueueAssigner.getModelingQueueNameForSubmission())
                .setCustomer("DellEbi_Warranty").setExportColumns(targetColumns).setNumMappers(1)
                .addExtraOption(optionalEnclosurePara).addExtraOption(optionalEnclosureValue).setSync(false).build();

        ApplicationId appId = sqoopJobService.exportData(exporter);

        log.info(String.format("Waiting for appId %s", appId));
        FinalApplicationStatus status = YarnUtils.waitFinalStatusForAppId(yarnConfiguration,
                appId, 600);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);

    }

    @Test(groups = "functional", enabled = true)
    public void exportDataToSQLServerWithDelimiter() throws Exception {
        URL inputUrl = ClassLoader
                .getSystemResource("com/latticeengines/sqoop/service/impl/files");

        String targetTable = "STG_SKU_MANUFACTURER";
        String sourceDir = "/tmp/SKU_Mfg_Dell_Delimiter.txt";
        List<String> targetColumns = Arrays.asList("MFG_NAME", "SUBCLASS_DESC", "ITM_SHRT_DESC", "MFG_PART_NUM");
        String optionalEnclosurePara = "--fields-terminated-by";
        String optionalEnclosureValue = "|~|";

        HdfsUtils.copyLocalToHdfs(yarnConfiguration, inputUrl.getPath() + "/SKU_Mfg_Dell_Delimiter.txt", "/tmp");

        SqoopExporter exporter = new SqoopExporter.Builder().setTable(targetTable).setSourceDir(sourceDir)
                .setDbCreds(getSQLServerCreds()).setQueue(LedpQueueAssigner.getModelingQueueNameForSubmission())
                .setCustomer("DellEbi_SKU_Mfg").setNumMappers(1).setExportColumns(targetColumns)
                .addExtraOption(optionalEnclosurePara).addExtraOption(optionalEnclosureValue).build();

        ApplicationId appId = sqoopJobService.exportData(exporter);

        log.info(String.format("Waiting for appId %s", appId));
        FinalApplicationStatus status = YarnUtils.waitFinalStatusForAppId(yarnConfiguration,
                appId, 600);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);

    }

    @Test(groups = "functional", enabled = true)
    public void exportDataToSQLServer() throws Exception {
        URL inputUrl = ClassLoader
                .getSystemResource("com/latticeengines/sqoop/service/impl/files");
        String targetTable = "STG_SKU_MANUFACTURER";
        String sourceDir = "/tmp/SKU_Mfg_Dell.txt";
        List<String> targetColumns = Arrays.asList("MFG_NAME", "SUBCLASS_DESC", "ITM_SHRT_DESC", "MFG_PART_NUM");

        HdfsUtils.copyLocalToHdfs(yarnConfiguration, inputUrl.getPath() + "/SKU_Mfg_Dell.txt", "/tmp");

        SqoopExporter exporter = new SqoopExporter.Builder().setTable(targetTable).setSourceDir(sourceDir)
                .setDbCreds(getSQLServerCreds()).setQueue(LedpQueueAssigner.getModelingQueueNameForSubmission())
                .setCustomer("DellEbi_SKU_Mfg").setNumMappers(1).setExportColumns(targetColumns).build();

        ApplicationId appId = sqoopJobService.exportData(exporter);

        log.info(String.format("Waiting for appId %s", appId));
        FinalApplicationStatus status = YarnUtils.waitFinalStatusForAppId(yarnConfiguration,
                appId, 600);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);

    }

    private DbCreds getSQLServerCreds() {

        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host(targetJdbcHost).port(Integer.parseInt(targetJdbcPort)).db(targetJdbcDb).user(targetJdbcUser)
                .clearTextPassword(targetJdbcPassword).dbType(targetJdbcType);
        DbCreds creds = new DbCreds(builder);
        return creds;
    }
}