package com.latticeengines.sqoop.service.impl;

import static org.testng.Assert.assertEquals;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.sqoop.exposed.service.SqoopJobService;
import com.latticeengines.sqoop.functionalframework.SqoopFunctionalTestNGBase;

public class SqoopJobServiceImplTestNG extends SqoopFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(SqoopJobServiceImplTestNG.class);

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private SqoopJobService sqoopJobService;

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

    @Test(groups = "functional")
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

        SqoopExporter exporter = new SqoopExporter.Builder() //
                .setTable(targetTable) //
                .setSourceDir(sourceDir) //
                .setDbCreds(getAuroraServerCreds()) //
                .setQueue(LedpQueueAssigner.getModelingQueueNameForSubmission()) //
                .setCustomer("DellEbi_Warranty") //
                .setExportColumns(targetColumns) //
                .setNumMappers(1) //
                .addExtraOption(optionalEnclosurePara) //
                .addExtraOption(optionalEnclosureValue) //
                .setSync(false) //
                .build();

        ApplicationId appId = sqoopJobService.exportData(exporter);
        waitForJobFinish(yarnConfiguration, appId);
    }

    @Test(groups = "functional")
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
                .setDbCreds(getAuroraServerCreds()).setQueue(LedpQueueAssigner.getModelingQueueNameForSubmission())
                .setCustomer("DellEbi_SKU_Mfg").setNumMappers(1).setExportColumns(targetColumns)
                .addExtraOption(optionalEnclosurePara).addExtraOption(optionalEnclosureValue).build();

        ApplicationId appId = sqoopJobService.exportData(exporter);

        log.info(String.format("Waiting for appId %s", appId));
        waitForJobFinish(yarnConfiguration, appId);

    }

    @Test(groups = "functional")
    public void exportDataToSQLServer() throws Exception {
        URL inputUrl = ClassLoader
                .getSystemResource("com/latticeengines/sqoop/service/impl/files");
        String targetTable = "STG_SKU_MANUFACTURER";
        String sourceDir = "/tmp/SKU_Mfg_Dell.txt";
        List<String> targetColumns = Arrays.asList("MFG_NAME", "SUBCLASS_DESC", "ITM_SHRT_DESC", "MFG_PART_NUM");

        HdfsUtils.copyLocalToHdfs(yarnConfiguration, inputUrl.getPath() + "/SKU_Mfg_Dell.txt", "/tmp");

        SqoopExporter exporter = new SqoopExporter.Builder().setTable(targetTable).setSourceDir(sourceDir)
                .setDbCreds(getAuroraServerCreds()).setQueue(LedpQueueAssigner.getModelingQueueNameForSubmission())
                .setCustomer("DellEbi_SKU_Mfg").setNumMappers(1).setExportColumns(targetColumns).build();

        ApplicationId appId = sqoopJobService.exportData(exporter);
        waitForJobFinish(yarnConfiguration, appId);
    }

    private void waitForJobFinish(Configuration yarnConfiguration, ApplicationId appId) {
        log.info(String.format("Waiting for appId %s", appId));
        @SuppressWarnings("deprecation")
        FinalApplicationStatus status = YarnUtils.waitFinalStatusForAppId(yarnConfiguration,
                appId, 600);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);
    }

    private DbCreds getAuroraServerCreds() {
        // FIXME: (YSong-M24) to be changed to dellebi properties file later
        String dbUrl = "jdbc:mysql://lpi-dev-cluster.cluster-ctigbumfbvzz.us-east-1.rds.amazonaws.com/DellEBI?autoReconnect=true&useSSL=false&user=$$USER$$&password=$$PASSWD$$";
        String dbUser = "LPI";
        String dbPassword = CipherUtils.decrypt("bi0mpJJNxiYpEka5C6JO4o75qVXoc80R7ma84i2eK5nKGejJiA0QY8p8RzlrlKU7");
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.jdbcUrl(dbUrl)
                .user(dbUser)
                .clearTextPassword(dbPassword)
                .dbType("MySQL");
        return new DbCreds(builder);
    }
}
