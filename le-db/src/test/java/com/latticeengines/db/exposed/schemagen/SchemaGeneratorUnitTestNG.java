package com.latticeengines.db.exposed.schemagen;

import static org.testng.Assert.assertTrue;

import java.io.File;

import org.testng.annotations.Test;

public class SchemaGeneratorUnitTestNG {

    @Test(groups = "unit")
    public void main() throws Exception {
        SchemaGenerator.main(new String[] { "PLS_MultiTenant", "com.latticeengines.domain.exposed.dataplatform" });
        assertTrue(new File("./ddl_PLS_MultiTenant_mysql5innodb.sql").exists());
        assertTrue(new File("./ddl_PLS_MultiTenant_sqlserver.sql").exists());
    }
}
