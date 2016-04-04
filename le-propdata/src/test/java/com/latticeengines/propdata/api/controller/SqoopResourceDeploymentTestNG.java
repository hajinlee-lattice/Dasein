package com.latticeengines.propdata.api.controller;

import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;
import com.latticeengines.domain.exposed.dataplatform.SqoopImporter;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.propdata.api.testframework.PropDataApiDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.propdata.InternalProxy;

@Component
public class SqoopResourceDeploymentTestNG extends PropDataApiDeploymentTestNGBase {

    @Autowired
    private InternalProxy sqoopProxy;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    @Qualifier(value = "propDataManageJdbcTemplate")
    private JdbcTemplate manageDbJdbcTemplate;

    private static final String SQL_TABLE_PREFIX = "SqoopTest";
    private static final String AVRO_DIR = "/tmp/propdatatest";
    private String sqlTable;
    private String jdbcUrl;

    @Value("${propdata.manage.url}")
    private String dbUrl;

    @Value("${propdata.manage.user}")
    private String dbUser;

    @Value("${propdata.manage.password.encrypted}")
    private String dbPassword;

    @Value("${propdata.manage.driver}")
    private String dbDriver;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        sqlTable = SQL_TABLE_PREFIX + "_" + testEnv;
        constructJdbcUrl();
        dropSqlTable();
        uploadTestAvro();
    }

    @AfterClass(groups = "deployment")
    public void tearDown() throws Exception {
        cleanupAvroDir();
        dropSqlTable();
    }

    @Test(groups = "deployment")
    public void testExport() {
        createSqlTable();
        DbCreds.Builder credsBuilder = new DbCreds.Builder();
        credsBuilder.jdbcUrl(jdbcUrl).driverClass(dbDriver).user(dbUser).password(dbPassword);
        SqoopExporter exporter = new SqoopExporter.Builder()
                .setCustomer("PropDataTest")
                .setNumMappers(4)
                .setTable(sqlTable)
                .setSourceDir(AVRO_DIR)
                .setDbCreds(new DbCreds(credsBuilder))
                .build();
        AppSubmission submission = sqoopProxy.exportTable(exporter);
        ApplicationId appId = ConverterUtils.toApplicationId(submission.getApplicationIds().get(0));
        FinalApplicationStatus finalStatus = YarnUtils.waitFinalStatusForAppId(yarnConfiguration, appId, 600);
        Assert.assertEquals(finalStatus, FinalApplicationStatus.SUCCEEDED);
    }

    @Test(groups = "deployment", dependsOnMethods = "testExport")
    public void testImport() {
        DbCreds.Builder credsBuilder = new DbCreds.Builder();
        credsBuilder.jdbcUrl(jdbcUrl).driverClass(dbDriver).user(dbUser).password(dbPassword);
        cleanupAvroDir();
        SqoopImporter impoter = new SqoopImporter.Builder()
                .setCustomer("PropDataTest")
                .setNumMappers(4)
                .setTable(sqlTable)
                .setTargetDir(AVRO_DIR)
                .setSplitColumn("LE_Last_Upload_Date")
                .setDbCreds(new DbCreds(credsBuilder))
                .build();
        AppSubmission submission = sqoopProxy.importTable(impoter);
        ApplicationId appId = ConverterUtils.toApplicationId(submission.getApplicationIds().get(0));
        FinalApplicationStatus finalStatus = YarnUtils.waitFinalStatusForAppId(yarnConfiguration, appId, 600);
        Assert.assertEquals(finalStatus, FinalApplicationStatus.SUCCEEDED);
    }

    private void uploadTestAvro() throws Exception {
        InputStream baseAvroStream = ClassLoader
                .getSystemResourceAsStream("com/latticeengines/propdata/api/controller/HGData.avro");
        cleanupAvroDir();
        HdfsUtils.mkdir(yarnConfiguration, AVRO_DIR);
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, baseAvroStream, AVRO_DIR + "/HGData.avro");
    }

    private void cleanupAvroDir() {
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, AVRO_DIR)) {
                HdfsUtils.rmdir(yarnConfiguration, AVRO_DIR);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void dropSqlTable() {
        if (dbDriver.toLowerCase().contains("mysql")) {
            manageDbJdbcTemplate.execute("DROP TABLE IF EXISTS `" + sqlTable + "`");
        } else {
            manageDbJdbcTemplate.execute("IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'"
                    + sqlTable + "') AND type in (N'U')) DROP TABLE " + sqlTable);
        }
    }

    private void createSqlTable() {
        if (dbDriver.toLowerCase().contains("mysql")) {
            String sql = "CREATE TABLE `" + sqlTable + "`(\n" +
                    "\t`Domain` VARCHAR(255) NOT NULL,\n" +
                    "\t`Supplier_Name` VARCHAR(255),\n" +
                    "\t`Segment_Name` VARCHAR(255),\n" +
                    "\t`HG_Category_1` VARCHAR(255),\n" +
                    "\t`HG_Category_2` VARCHAR(255),\n" +
                    "\t`HG_Category_1_Parent` VARCHAR(255),\n" +
                    "\t`HG_Category_2_Parent` VARCHAR(255),\n" +
                    "\t`Creation_Date` DATETIME,\n" +
                    "\t`Last_Verified_Date` DATETIME,\n" +
                    "\t`LE_Last_Upload_Date` DATETIME,\n" +
                    "\t`Location_Count` INTEGER,\n" +
                    "\t`Max_Location_Intensity` INTEGER) ENGINE=InnoDB;";
            manageDbJdbcTemplate.execute(sql);
        } else {
            String sql = "CREATE TABLE [" + sqlTable + "](\n" +
                    "\t[Domain] [nvarchar](255) NOT NULL,\n" +
                    "\t[Supplier_Name] [nvarchar](255),\n" +
                    "\t[Segment_Name] [nvarchar](255),\n" +
                    "\t[HG_Category_1] [nvarchar](255),\n" +
                    "\t[HG_Category_2] [nvarchar](255),\n" +
                    "\t[HG_Category_1_Parent] [nvarchar](255),\n" +
                    "\t[HG_Category_2_Parent] [nvarchar](255),\n" +
                    "\t[Creation_Date] [DATETIME],\n" +
                    "\t[Last_Verified_Date] [DATETIME],\n" +
                    "\t[LE_Last_Upload_Date] [DATETIME],\n" +
                    "\t[Location_Count] [INT],\n" +
                    "\t[Max_Location_Intensity] [INT])";
            manageDbJdbcTemplate.execute(sql);
        }
    }

    private void constructJdbcUrl() {
        jdbcUrl = dbUrl;
        if (dbDriver.toLowerCase().contains("mysql")) {
            jdbcUrl += "?user=$$USER$$&password=$$PASSWD$$";
        } else {
            if (!jdbcUrl.endsWith(";")) {
                jdbcUrl += ";";
            }
            jdbcUrl += "user=$$USER$$;password=$$PASSWD$$";
        }
    }

}
