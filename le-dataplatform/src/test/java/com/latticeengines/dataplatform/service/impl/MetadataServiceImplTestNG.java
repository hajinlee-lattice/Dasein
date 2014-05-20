package com.latticeengines.dataplatform.service.impl;

import static org.testng.Assert.assertEquals;

import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.dataplatform.service.MetadataService;
import com.latticeengines.domain.exposed.dataplatform.DbCreds;

public class MetadataServiceImplTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private MetadataService metadataService;

    @Override
    protected boolean doYarnClusterSetup() {
        return false;
    }

    @Test(groups = "functional", enabled = true)
    public void createDataSchema() {
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host("10.41.1.240") //
                .db("MuleSoft") //
                .port(1433) //
                .user("DataLoader_Dep") //
                .password("L@ttice1");

        DbCreds creds = new DbCreds(builder);

        Schema avroSchema = metadataService.getAvroSchema(creds, "mulesoft");
        System.out.println(avroSchema.toString(true));
        assertEquals(avroSchema.getFields().size(), 203);
        System.out.println(avroSchema.toString(true));
    }
}
