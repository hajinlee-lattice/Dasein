package com.latticeengines.datadb.exposed.schemagen;

import static org.testng.Assert.assertTrue;

import java.io.File;

import org.testng.annotations.Test;

import com.latticeengines.db.exposed.schemagen.SchemaGenerator;

public class SchemaGeneratorUnitTestNG {

    @Test(groups = "unit")
    public void recommendationSchemaGen() throws Exception {
        SchemaGenerator.main(new String[] { "Data_MultiTenant", "com.latticeengines.domain.exposed.playmakercore" });
        assertTrue(new File("./ddl_data_multitenant_mysql5innodb.sql").exists());
        assertTrue(new File("./ddl_data_multitenant_sqlserver.sql").exists());
    }

    @Test(groups = "unit")
    public void activityAlertSchemaGen() throws Exception {
        File mySqlfile = new File("./ddl_data_multitenant_mysql5innodb.sql");
        File sqlServerfile = new File("./ddl_data_multitenant_sqlserver.sql");

        if (mySqlfile.exists())
            mySqlfile.delete();
        if (sqlServerfile.exists())
            sqlServerfile.delete();

        SchemaGenerator.main(new String[] { "Data_MultiTenant", "com.latticeengines.domain.exposed.cdl.activitydata" });
        assertTrue(mySqlfile.exists());
        assertTrue(sqlServerfile.exists());
    }
}
