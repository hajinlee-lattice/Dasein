package com.latticeengines.dataplatform.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.net.URL;
import java.sql.Types;
import java.util.AbstractMap;

import org.apache.avro.Schema;
import org.apache.derby.drda.NetworkServerControl;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.exposed.service.MetadataService;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.domain.exposed.modeling.DataSchema;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.modeling.Field;

public class MetadataServiceImplTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private MetadataService metadataService;
    
    private NetworkServerControl serverControl;
    
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        serverControl = new NetworkServerControl();
        serverControl.start(null);
    }
    
    
    @AfterClass(groups = "functional")
    public void tearDown() throws Exception {
        serverControl.shutdown();
    }

    @SuppressWarnings("deprecation")
    @Test(groups = { "functional", "functional.production" }, enabled = true)
    public void getDataTypes() {
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host("10.41.1.250") //
                .db("SP_7_Tests") //
                .port(1433) //
                .user("root") //
                .password("welcome");

        DbCreds creds = new DbCreds(builder);

        DataSchema schema = metadataService.createDataSchema(creds, "Play_11_Training_WithRevenue");
        Schema avroSchema = metadataService.getAvroSchema(creds, "Play_11_Training_WithRevenue");

        for (Field field : schema.getFields()) {
            String fieldName = field.getName();
            org.apache.avro.Schema.Field avroField = avroSchema.getField(fieldName);
            System.out.println("Field " + field.getName() + " with avro type " + avroField.schema().toString()
                    + " and sql type = " + field.getSqlType());

            if (fieldName.equals("Ext_LEAccount_PD_FundingDateUpdated")) {
                assertEquals(field.getSqlType(), Types.TIMESTAMP);
            }
            if (fieldName.equals("Ext_LEAccount_PD_Timestamp")) {
                assertEquals(field.getSqlType(), Types.BINARY);
            }
            if (fieldName.equals("Ext_LEAccount_PD_Time")) {
                assertEquals(field.getSqlType(), Types.TIME);
            }
        }

    }
    
    @Test(groups = { "functional" }, enabled = true)
    public void getJdbcConnectionUrlUsingUrl() {
        String url = "jdbc:sqlserver://10.41.1.250:1433;databaseName=SP_7_Tests;user=root;password=welcome";
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.jdbcUrl(url);

        DbCreds creds = new DbCreds(builder);

        assertEquals(metadataService.getJdbcConnectionUrl(creds), url);
    }

    @Test(groups = { "functional" }, enabled = true)
    public void getJdbcConnectionUrlUsingUrlAndDriverClass() {
        String url = "jdbc:derby://localhost:1527/testdb;create=true";
        String driver = "org.apache.derby.jdbc.EmbeddedDriver";
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.jdbcUrl(url).driverClass(driver);
        DbCreds creds = new DbCreds(builder);

        assertEquals(metadataService.getJdbcConnectionUrl(creds), url);
    }

    @Test(groups = { "functional" }, enabled = true)
    public void getJdbcConnectionUrlUsingUrlAndDriverClassForFile() throws Exception {
        AbstractMap.SimpleEntry<DbCreds, String> dbInfo = buildCredsForFile();
        assertEquals(metadataService.getJdbcConnectionUrl(dbInfo.getKey()), dbInfo.getValue());
    }

    @Test(groups = { "functional" }, enabled = true)
    public void createDataSchema() throws Exception {
        AbstractMap.SimpleEntry<DbCreds, String> dbInfo = buildCredsForFile();
        DataSchema schema = metadataService.createDataSchema(dbInfo.getKey(), "Nutanix");
        assertTrue(schema.getFields().size() > 0);
    }
    
    private AbstractMap.SimpleEntry<DbCreds, String> buildCredsForFile() {
        URL inputUrl = ClassLoader.getSystemResource("com/latticeengines/dataplatform/service/impl/sqoopSyncJobServiceImpl");
        String url = String.format("jdbc:relique:csv:%s", inputUrl.getPath());
        String driver = "org.relique.jdbc.csv.CsvDriver";
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.jdbcUrl(url).driverClass(driver).dbType("GenericJDBC");
        DbCreds creds = new DbCreds(builder);
        return new AbstractMap.SimpleEntry<DbCreds, String>(creds, url);
    }
}
