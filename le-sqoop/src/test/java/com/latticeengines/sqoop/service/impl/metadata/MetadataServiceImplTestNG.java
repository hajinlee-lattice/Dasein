package com.latticeengines.sqoop.service.impl.metadata;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.net.URL;
import java.sql.Types;
import java.util.AbstractMap;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.derby.drda.NetworkServerControl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.service.DbMetadataService;
import com.latticeengines.domain.exposed.modeling.DataSchema;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.modeling.Field;
import com.latticeengines.sqoop.exposed.service.SqoopMetadataService;
import com.latticeengines.sqoop.functionalframework.SqoopFunctionalTestNGBase;

public class MetadataServiceImplTestNG extends SqoopFunctionalTestNGBase {

    @Inject
    private DbMetadataService dbMetadataService;

    @Inject
    private SqoopMetadataService sqoopMetadataService;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        NetworkServerControl serverControl = new NetworkServerControl();
        serverControl.start(null);
    }

    @AfterClass(groups = "functional")
    public void tearDown() throws Exception {
        // serverControl.shutdown();
    }

    @Test(groups = { "functional" }, enabled = true)
    public void getDataTypes() {
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host("10.41.1.250") //
                .db("SP_7_Tests") //
                .port(1433) //
                .user("root") //
                .clearTextPassword("welcome");

        DbCreds creds = new DbCreds(builder);

        DataSchema schema = sqoopMetadataService.createDataSchema(creds, "Play_11_Training_WithRevenue");
        Schema avroSchema = sqoopMetadataService.getAvroSchema(creds, "Play_11_Training_WithRevenue");

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

        assertEquals(dbMetadataService.getJdbcConnectionUrl(creds), url);
        assertEquals(dbMetadataService.getConnectionUrl(creds),
                "jdbc:sqlserver://10.41.1.250:1433;databaseName=SP_7_Tests");
        assertEquals(dbMetadataService.getConnectionUserName(creds), "root");
        assertEquals(dbMetadataService.getConnectionPassword(creds), "welcome");
    }

    @Test(groups = { "functional" }, enabled = true)
    public void getJdbcConnectionUrlUsingUrlAndDriverClass() {
        String url = "jdbc:derby://localhost:1527/testdb;create=true";
        String driver = "org.apache.derby.jdbc.EmbeddedDriver";
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.jdbcUrl(url).driverClass(driver);
        DbCreds creds = new DbCreds(builder);

        assertEquals(dbMetadataService.getJdbcConnectionUrl(creds), url);
    }

    @Test(groups = { "functional" }, enabled = true)
    public void getJdbcConnectionUrlUsingUrlAndDriverClassForFile() throws Exception {
        AbstractMap.SimpleEntry<DbCreds, String> dbInfo = buildCredsForFile();
        assertEquals(dbMetadataService.getJdbcConnectionUrl(dbInfo.getKey()), dbInfo.getValue());
    }

    @Test(groups = { "functional" }, enabled = false)
    public void createDataSchema() throws Exception {
        AbstractMap.SimpleEntry<DbCreds, String> dbInfo = buildCredsForFile();
        DataSchema schema = sqoopMetadataService.createDataSchema(dbInfo.getKey(), "Nutanix");
        assertTrue(schema.getFields().size() > 0);
    }

    private AbstractMap.SimpleEntry<DbCreds, String> buildCredsForFile() {
        URL inputUrl = ClassLoader
                .getSystemResource("com/latticeengines/sqoop/service/impl/files");
        String url = String.format("jdbc:relique:csv:%s", inputUrl.getPath());
        String driver = "org.relique.jdbc.csv.CsvDriver";
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.jdbcUrl(url).driverClass(driver).dbType("GenericJDBC");
        DbCreds creds = new DbCreds(builder);
        return new AbstractMap.SimpleEntry<DbCreds, String>(creds, url);
    }
}
