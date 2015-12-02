package com.latticeengines.domain.exposed.propdata;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public class MatchClientSerializer extends JsonSerializer<MatchClient> {

    @Override
    public void serialize(MatchClient value, JsonGenerator generator,
                          SerializerProvider provider) throws IOException {

        generator.writeStartObject();
        generator.writeFieldName("Name");
        generator.writeString(value.name());
        generator.writeFieldName("Url");
        generator.writeString(String.format("jdbc:sqlserver://%s:%d;databaseName=PropDataMatchDB;",
                value.getHost(), value.getPort()));
        generator.writeFieldName("Username");
        generator.writeString("DLTransfer");
        generator.writeFieldName("EncryptedPassword");
        generator.writeString("Q1nh4HIYGkg4OnQIEbEuiw==");
        generator.writeEndObject();
    }
}
