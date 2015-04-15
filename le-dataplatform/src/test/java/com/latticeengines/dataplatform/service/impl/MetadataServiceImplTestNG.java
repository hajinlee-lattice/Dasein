package com.latticeengines.dataplatform.service.impl;

import static org.testng.Assert.assertEquals;

import java.sql.Types;

import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.dataplatform.exposed.service.MetadataService;
import com.latticeengines.dataplatform.functionalframework.DataPlatformFunctionalTestNGBase;
import com.latticeengines.domain.exposed.modeling.DataSchema;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.modeling.Field;

public class MetadataServiceImplTestNG extends DataPlatformFunctionalTestNGBase {

    @Autowired
    private MetadataService metadataService;

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
}
