package com.latticeengines.db.exposed.schemagen;

import static org.testng.Assert.assertTrue;

import java.io.File;

import org.testng.annotations.Test;

public class SchemaGeneratorUnitTestNG {

    @Test(groups = "unit")
    public void main() throws Exception {
        SchemaGenerator.main(new String[] { "pls", //
                "com.latticeengines.domain.exposed.pls", //
                "com.latticeengines.domain.exposed.security" });
        assertTrue(new File("./ddl_pls_mysql5innodb.sql").exists());
        assertTrue(new File("./ddl_pls_sqlserver.sql").exists());
    }
}
