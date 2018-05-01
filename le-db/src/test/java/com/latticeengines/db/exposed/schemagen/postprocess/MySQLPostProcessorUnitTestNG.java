package com.latticeengines.db.exposed.schemagen.postprocess;

import org.testng.Assert;
import org.testng.annotations.Test;

public class MySQLPostProcessorUnitTestNG {

    @Test(groups = "unit")
    private void testGeneratedColumn() {
        String line = "create table `DataUnit` (`UUID` varchar(36) not null, `CreatedDate` datetime not null, `Document` `'JSON'`, `LastModifiedDate` datetime not null, `TenantId` varchar(255) not null, `Name` `'VARCHAR(100) GENERATED ALWAYS AS (`Document` ->> '$.Name')'` not null, primary key (`UUID`)) engine=InnoDB;";
        MySQLPostProcessor.FixCustomColumnDefinition processor = new MySQLPostProcessor.FixCustomColumnDefinition();
        String processed = processor.processLine(line).get(0);
        Assert.assertFalse(processed.contains("`'"));
        Assert.assertFalse(processed.contains("'`"));
    }

}
