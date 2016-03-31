package com.latticeengines.domain.exposed.propdata;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

public class MatchClientDocumentUnitTestNG {

    @Test(groups = "unit")
    public void testSerDe() throws IOException {
        MatchClient client = MatchClient.PD128;

        MatchClientDocument doc = new MatchClientDocument(client);
        Assert.assertTrue(doc.getUrl().contains(client.host));

        ObjectMapper mapper = new ObjectMapper();
        String serializedStr = mapper.writeValueAsString(doc);

        MatchClientDocument deDoc = mapper.readValue(serializedStr, MatchClientDocument.class);

        Assert.assertEquals(doc.getMatchClient(), deDoc.getMatchClient());
    }

    @Test(groups = "unit")
    public void testDefaultValues() throws IOException {
        MatchClient client = MatchClient.PD128;

        MatchClientDocument doc = new MatchClientDocument(client);
        ObjectMapper mapper = new ObjectMapper();
        String serializedStr = mapper.writeValueAsString(doc);

        Assert.assertTrue(serializedStr.contains(
                "\"Username\":\"DLTransfer\",\"EncryptedPassword\":\"Q1nh4HIYGkg4OnQIEbEuiw==\""
        ));
    }

}
