package com.latticeengines.dataplatform.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.exposed.service.MetadataService;
import com.latticeengines.dataplatform.exposed.service.ModelingService;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.modeling.DataProfileConfiguration;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.modeling.LoadConfiguration;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;

public class ModelingServiceValidationTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private ModelingService modelingService;

    @Autowired
    private MetadataService metadataService;

    @Value("${dataplatform.dlorchestration.datasource.host}")
    private String dataSourceHost;

    @Value("${dataplatform.dlorchestration.datasource.port}")
    private String dataSourcePort;

    @Value("${dataplatform.dlorchestration.datasource.dbname}")
    private String dataSourceDB;

    @Value("${dataplatform.dlorchestration.datasource.type}")
    private String dataSourceDBType;

    @Value("${dataplatform.dlorchestration.datasource.user}")
    private String dataSourceUser;

    @Value("${dataplatform.dlorchestration.datasource.password.encrypted}")
    private String dataSourcePasswd;

    private LoadConfiguration config;

    private JdbcTemplate jdbcTemplate;

    @SuppressWarnings("deprecation")
    @BeforeClass(groups = { "functional", "functional.production" })
    public void setup() {
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host(dataSourceHost).port(Integer.parseInt(dataSourcePort)).db(dataSourceDB).dbType(dataSourceDBType)
                .user(dataSourceUser).password(dataSourcePasswd);
        DbCreds creds = new DbCreds(builder);

        config = new LoadConfiguration();
        config.setCreds(creds);
        config.setTable("TestTableWithBadColumnNames");

        jdbcTemplate = metadataService.constructJdbcTemplate(creds);
        dropTable(config.getTable());
    }

    private void createTable(String table, String columnName) {
        if (dataSourceDBType.equals("MySQL")) {
            jdbcTemplate.execute("Create table `" + table + "` ( `" + columnName + "` bigint)");
        } else if (dataSourceDBType.equals("SQLServer")) {
            jdbcTemplate.execute("Create table [" + table + "] ( [" + columnName + "] bigint)");
        }
    }

    private void dropTable(String table) {
        metadataService.dropTable(jdbcTemplate, table);
    }

    @Test(groups = "functional")
    public void validateLoadWithInvalidCustomerName() {
        config.setCustomer("{Dell}");
        LedpException ex = null;
        try {
            modelingService.loadData(config);
        } catch (LedpException ex2) {
            ex = ex2;
        }
        Assert.assertTrue(ex instanceof LedpException);
        Assert.assertEquals(ex.getCode(), LedpCode.LEDP_10007);
    }

    @Test(groups = "functional", dataProvider = "validateColumnNames")
    public void validateLoadWithInvalidColumnNames(String columnName) {
        config.setCustomer("Dell");
        createTable(config.getTable(), columnName);

        LedpException ex = null;
        try {
            modelingService.loadData(config);
        } catch (LedpException ex2) {
            ex = ex2;
        }
        Assert.assertTrue(ex instanceof LedpException);
        Assert.assertEquals(ex.getCode(), LedpCode.LEDP_10007);
        dropTable(config.getTable());
    }

    @Test(groups = "functional")
    public void validateCreateSamples() {
        validateSamplingConfigCustomerField();
        validateSamplingConfigTableField();
        validateSamplingConfigNotNullFields();
    }

    private void validateSamplingConfigCustomerField() {
        SamplingConfiguration config = new SamplingConfiguration();
        config.setCustomer("{Dell}");
        LedpException ex = null;
        try {
            modelingService.createSamples(config);
        } catch (LedpException ex2) {
            ex = ex2;
        }
        Assert.assertTrue(ex instanceof LedpException);
        Assert.assertEquals(ex.getCode(), LedpCode.LEDP_10007);
    }

    private void validateSamplingConfigTableField() {
        SamplingConfiguration config = new SamplingConfiguration();
        config.setCustomer("Dell");
        config.setTable("");
        LedpException ex = null;
        try {
            modelingService.createSamples(config);
        } catch (LedpException ex2) {
            ex = ex2;
        }
        Assert.assertTrue(ex instanceof LedpException);
        Assert.assertEquals(ex.getCode(), LedpCode.LEDP_10007);
    }

    private void validateSamplingConfigNotNullFields() {
        SamplingConfiguration config = new SamplingConfiguration();
        config.setTable("Table");
        LedpException ex = null;
        try {
            modelingService.createSamples(config);
        } catch (LedpException ex2) {
            ex = ex2;
        }
        Assert.assertTrue(ex instanceof LedpException);
        Assert.assertEquals(ex.getCode(), LedpCode.LEDP_15012);
    }

    @Test(groups = "functional")
    public void validateProfileData() {
        DataProfileConfiguration config = new DataProfileConfiguration();
        config.setTable("{Dell}");
        LedpException ex = null;
        try {
            modelingService.profileData(config);
        } catch (LedpException ex2) {
            ex = ex2;
        }
        Assert.assertTrue(ex instanceof LedpException);
        Assert.assertEquals(ex.getCode(), LedpCode.LEDP_10007);
    }

    @Test(groups = "functional")
    public void validateSubmitModel() {
        Model model = new Model();
        model.setCustomer("{Dell}");
        LedpException ex = null;
        try {
            modelingService.submitModel(model);
        } catch (LedpException ex2) {
            ex = ex2;
        }
        Assert.assertTrue(ex instanceof LedpException);
        Assert.assertEquals(ex.getCode(), LedpCode.LEDP_10007);
    }

    @DataProvider(name = "validateColumnNames")
    public static Object[][] getValidateColumnNames() {
        return new Object[][] { { " badcolumnname"}, //
                { "bad:column:name"}, //
                { "bad/column/name"}, //
        };
    }
}
