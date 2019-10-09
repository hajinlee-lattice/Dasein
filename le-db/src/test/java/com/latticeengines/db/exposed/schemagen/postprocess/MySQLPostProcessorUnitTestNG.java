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

    @Test(groups = "unit")
    private void testDefaultColumns() {
        String line = "create table `SOME_TABLE` (`PID` bigint not null auto_increment, `BUCKETS_TO_LAUNCH` longtext, `CHANNEL_CONFIG` longtext, \n"
                + "`DELETED` bit not null, `EXPIRATION_DATE` `'DOUBLE(16,2) DEFAULT '0.00''`, `ID` `'VARCHAR(10) DEFAULT '''` not null, `ALWAYS_ON` `'BIT DEFAULT 0'` not null, \n"
                + "`FK_PLAY_ID` bigint not null, `FK_TENANT_ID` bigint not null, primary key (`PID`)) engine=InnoDB;";
        MySQLPostProcessor.FixCustomColumnDefinition processor = new MySQLPostProcessor.FixCustomColumnDefinition();
        String processed = processor.processLine(line).get(0);
        Assert.assertFalse(processed.contains("`'"));
        Assert.assertFalse(processed.contains("'`"));
    }

}
