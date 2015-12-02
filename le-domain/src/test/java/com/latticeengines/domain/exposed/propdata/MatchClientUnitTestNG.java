package com.latticeengines.domain.exposed.propdata;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

public class MatchClientUnitTestNG {

    @Test(groups = "unit")
    public void testSerDe() throws IOException {
        MatchClient client = MatchClient.PD127;

        ObjectMapper mapper = new ObjectMapper();
        String serializedStr = mapper.writeValueAsString(client);

        Assert.assertEquals(serializedStr, "{\"Name\":\"PD127\"," +
                "\"Url\":\"jdbc:sqlserver://10.51.15.127:1433;databaseName=PropDataMatchDB;\"," +
                "\"Username\":\"DLTransfer\",\"EncryptedPassword\":\"Q1nh4HIYGkg4OnQIEbEuiw==\"}");

        MatchClient deserializedClient = mapper.readValue(serializedStr, MatchClient.class);
        Assert.assertEquals(deserializedClient, client);
    }

}
