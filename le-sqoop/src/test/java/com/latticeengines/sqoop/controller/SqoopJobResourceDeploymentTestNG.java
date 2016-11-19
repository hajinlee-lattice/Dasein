package com.latticeengines.sqoop.controller;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;
import com.latticeengines.domain.exposed.dataplatform.SqoopImporter;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.proxy.exposed.sqoop.SqoopProxy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-sqoop-context.xml" })
public class SqoopJobResourceDeploymentTestNG extends AbstractTestNGSpringContextTests {

    @Autowired
    private SqoopProxy sqoopProxy;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    @Qualifier(value = "sqoopTestJdbcTemplate")
    private JdbcTemplate jdbcTemplate;

    private static final String SQL_TABLE_PREFIX = "SqoopTest";
    private static final String TEST_CUSTOMER = "SqoopTest";
    private static final String TEST_DIR = "/tmp/sqooptest";
    private static final String AVRO_DIR = "/tmp/sqooptest/avro";
    private static final String AVRO_FILE = "files/HGData.avro";
    private static final String CSV_DIR = "/tmp/sqooptest/csv";
    private static final String CSV_FILE = "files/CacheSeed.csv.gz";
    private String sqlTable;
    private String sqlTableForCsv;
    private String jdbcUrl;
    private String sqlQuery;

    @Value("${datacloud.manage.url}")
    private String dbUrl;

    @Value("${datacloud.manage.user}")
    private String dbUser;

    @Value("${datacloud.manage.password.encrypted}")
    private String dbPassword;

    @Value("${datacloud.manage.driver}")
    private String dbDriver;

    @Value("${common.test.env}")
    private String testEnv;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        sqlTable = SQL_TABLE_PREFIX + "_" + testEnv;
        sqlTableForCsv = SQL_TABLE_PREFIX + "_Csv_" + testEnv;
        sqlQuery = "SELECT '' AS Duns, Domain, Name AS Company_Name, '' AS Street_Address_1, "
                + "'' AS Street_Address_2, City AS Company_City, State AS Company_State, Country AS Company_Country, "
                + "'' AS Company_Phone, SeedID AS LatticeID FROM " + sqlTableForCsv
                + " WHERE $CONDITIONS";
        constructJdbcUrl();
        dropSqlTable();
        uploadTestFile(AVRO_DIR, AVRO_FILE);
        uploadTestFile(CSV_DIR, CSV_FILE);
        createSqlTable();
    }

    @AfterClass(groups = "deployment")
    public void tearDown() throws Exception {
        cleanupHdfsDir(TEST_DIR);
        dropSqlTable();
    }

    @Test(groups = "deployment")
    public void testExport() {
        DbCreds.Builder credsBuilder = new DbCreds.Builder();
        credsBuilder //
                .jdbcUrl(jdbcUrl) //
                .driverClass(dbDriver) //
                .user(dbUser) //
                .encryptedPassword(CipherUtils.encrypt(dbPassword));
        SqoopExporter exporter = new SqoopExporter.Builder() //
                .setCustomer(TEST_CUSTOMER) //
                .setNumMappers(4) //
                .setTable(sqlTable) //
                .setSourceDir(AVRO_DIR) //
                .setQueue(LedpQueueAssigner.getPropDataQueueNameForSubmission()) //
                .setDbCreds(new DbCreds(credsBuilder)).build();
        AppSubmission submission = sqoopProxy.exportData(exporter);
        ApplicationId appId = ConverterUtils.toApplicationId(submission.getApplicationIds().get(0));
        FinalApplicationStatus finalStatus = YarnUtils.waitFinalStatusForAppId(yarnConfiguration,
                appId, 600);
        Assert.assertEquals(finalStatus, FinalApplicationStatus.SUCCEEDED);
    }

    @Test(groups = "deployment", dependsOnMethods = "testExport")
    public void testImport() {
        DbCreds.Builder credsBuilder = new DbCreds.Builder();
        credsBuilder //
                .jdbcUrl(jdbcUrl) //
                .driverClass(dbDriver) //
                .user(dbUser) //
                .encryptedPassword(CipherUtils.encrypt(dbPassword));
        cleanupHdfsDir(AVRO_DIR);
        SqoopImporter impoter = new SqoopImporter.Builder() //
                .setCustomer(TEST_CUSTOMER) //
                .setNumMappers(4) //
                .setTable(sqlTable) //
                .setTargetDir(AVRO_DIR) //
                .setQueue(LedpQueueAssigner.getPropDataQueueNameForSubmission()) //
                .setSplitColumn("LE_Last_Upload_Date") //
                .setDbCreds(new DbCreds(credsBuilder)) //
                .build();
        AppSubmission submission = sqoopProxy.importData(impoter);
        ApplicationId appId = ConverterUtils.toApplicationId(submission.getApplicationIds().get(0));
        FinalApplicationStatus finalStatus = YarnUtils.waitFinalStatusForAppId(yarnConfiguration,
                appId, 600);
        Assert.assertEquals(finalStatus, FinalApplicationStatus.SUCCEEDED);
    }

    @Test(groups = "deployment")
    public void testExportCsv() {
        DbCreds.Builder credsBuilder = new DbCreds.Builder();
        credsBuilder //
                .jdbcUrl(jdbcUrl) //
                .driverClass(dbDriver) //
                .user(dbUser) //
                .encryptedPassword(CipherUtils.encrypt(dbPassword));
        SqoopExporter exporter = new SqoopExporter.Builder() //
                .setCustomer("PropDataTest") //
                .setNumMappers(1) //
                .setTable(sqlTableForCsv) //
                .setSourceDir(CSV_DIR) //
                .setDbCreds(new DbCreds(credsBuilder)) //
                .addExtraOption("--input-optionally-enclosed-by") //
                .addExtraOption("\"") //
                .setQueue(LedpQueueAssigner.getPropDataQueueNameForSubmission()) //
                .build();
        AppSubmission submission = sqoopProxy.exportData(exporter);
        ApplicationId appId = ConverterUtils.toApplicationId(submission.getApplicationIds().get(0));
        FinalApplicationStatus finalStatus = YarnUtils.waitFinalStatusForAppId(yarnConfiguration,
                appId, 600);
        Assert.assertEquals(finalStatus, FinalApplicationStatus.SUCCEEDED);
    }

    @Test(groups = "deployment", dependsOnMethods = "testExportCsv")
    public void testImportCsv() {
        DbCreds.Builder credsBuilder = new DbCreds.Builder();
        credsBuilder //
                .jdbcUrl(jdbcUrl) //
                .driverClass(dbDriver) //
                .user(dbUser) //
                .encryptedPassword(CipherUtils.encrypt(dbPassword));
        cleanupHdfsDir(CSV_DIR);
        SqoopImporter importer = new SqoopImporter.Builder() //
                .setCustomer(TEST_CUSTOMER) //
                .setNumMappers(1) //
                .setTable(sqlTableForCsv) //
                .setTargetDir(CSV_DIR) //
                .setDbCreds(new DbCreds(credsBuilder)) //
                .setQuery(sqlQuery) //
                .setSplitColumn("SeedID") //
                .setMode(SqoopImporter.Mode.QUERY) //
                .setNumMappers(1) //
                .build();
        List<String> otherOptions = new ArrayList<>(
                Arrays.asList("--relaxed-isolation", "--as-textfile"));
        otherOptions.add("--optionally-enclosed-by");
        otherOptions.add("\"");
        otherOptions.add("--fields-terminated-by");
        otherOptions.add(",");
        importer.setOtherOptions(otherOptions);
        AppSubmission submission = sqoopProxy.importData(importer);
        ApplicationId appId = ConverterUtils.toApplicationId(submission.getApplicationIds().get(0));
        FinalApplicationStatus finalStatus = YarnUtils.waitFinalStatusForAppId(yarnConfiguration,
                appId, 600);
        Assert.assertEquals(finalStatus, FinalApplicationStatus.SUCCEEDED);
    }

    private void uploadTestFile(String hdfsDir, String localFile) throws Exception {
        InputStream baseStream = ClassLoader.getSystemResourceAsStream(localFile);
        cleanupHdfsDir(hdfsDir);
        HdfsUtils.mkdir(yarnConfiguration, hdfsDir);
        Path localFilePath = new Path(localFile);
        Path hdflFilePath = new Path(hdfsDir, localFilePath.getName());
        HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, baseStream, hdflFilePath.toString());
    }

    private void cleanupHdfsDir(String dir) {
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, dir)) {
                HdfsUtils.rmdir(yarnConfiguration, dir);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void dropSqlTable() {
        if (dbDriver.toLowerCase().contains("mysql")) {
            jdbcTemplate.execute("DROP TABLE IF EXISTS `" + sqlTable + "`");
            jdbcTemplate.execute("DROP TABLE IF EXISTS `" + sqlTableForCsv + "`");
        } else {
            jdbcTemplate
                    .execute("IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'"
                            + sqlTable + "') AND type in (N'U')) DROP TABLE " + sqlTable);
            jdbcTemplate
                    .execute("IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'"
                            + sqlTableForCsv + "') AND type in (N'U')) DROP TABLE "
                            + sqlTableForCsv);
        }
    }

    private void createSqlTable() {
        if (dbDriver.toLowerCase().contains("mysql")) {
            String sql = "CREATE TABLE `" + sqlTable + "`(\n"
                    + "\t`Domain` VARCHAR(255) NOT NULL,\n" + "\t`Supplier_Name` VARCHAR(255),\n"
                    + "\t`Segment_Name` VARCHAR(255),\n" + "\t`HG_Category_1` VARCHAR(255),\n"
                    + "\t`HG_Category_2` VARCHAR(255),\n"
                    + "\t`HG_Category_1_Parent` VARCHAR(255),\n"
                    + "\t`HG_Category_2_Parent` VARCHAR(255),\n" + "\t`Creation_Date` DATETIME,\n"
                    + "\t`Last_Verified_Date` DATETIME,\n" + "\t`LE_Last_Upload_Date` DATETIME,\n"
                    + "\t`Location_Count` INTEGER,\n"
                    + "\t`Max_Location_Intensity` INTEGER) ENGINE=InnoDB;";
            jdbcTemplate.execute(sql);
            sql = "CREATE TABLE " + sqlTableForCsv + "(" + "Domain VARCHAR(200) NULL,"
                    + "Name VARCHAR(500) NULL," + "City VARCHAR(200) NULL,"
                    + "State VARCHAR(200) NULL," + "Country VARCHAR(200) NULL,"
                    + "InsideViewID BIGINT NULL," + "SeedID BIGINT NOT NULL," + "Employee INT NULL,"
                    + "Revenue BIGINT NULL," + "BusinessIndustry VARCHAR(200) NULL) ENGINE=InnoDB;";
            jdbcTemplate.execute(sql);
        } else {
            String sql = "CREATE TABLE [" + sqlTable + "](\n"
                    + "\t[Domain] [nvarchar](255) NOT NULL,\n"
                    + "\t[Supplier_Name] [nvarchar](255),\n" + "\t[Segment_Name] [nvarchar](255),\n"
                    + "\t[HG_Category_1] [nvarchar](255),\n"
                    + "\t[HG_Category_2] [nvarchar](255),\n"
                    + "\t[HG_Category_1_Parent] [nvarchar](255),\n"
                    + "\t[HG_Category_2_Parent] [nvarchar](255),\n"
                    + "\t[Creation_Date] [DATETIME],\n" + "\t[Last_Verified_Date] [DATETIME],\n"
                    + "\t[LE_Last_Upload_Date] [DATETIME],\n" + "\t[Location_Count] [INT],\n"
                    + "\t[Max_Location_Intensity] [INT])";
            jdbcTemplate.execute(sql);
            sql = "CREATE TABLE [" + sqlTableForCsv + "](" + "[Domain] [nvarchar](200) NULL,"
                    + "[Name] [varchar](500) NULL," + "[City] [varchar](200) NULL,"
                    + "[State] [varchar](200) NULL," + "[Country] [varchar](200) NULL,"
                    + "[InsideViewID] [bigint] NULL," + "[SeedID] [bigint] NOT NULL,"
                    + "[Employee] [int] NULL," + "[Revenue] [bigint] NULL,"
                    + "[BusinessIndustry] [varchar](200) NULL)";
            jdbcTemplate.execute(sql);
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
