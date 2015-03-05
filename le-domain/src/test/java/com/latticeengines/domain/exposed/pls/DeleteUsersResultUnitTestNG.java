package com.latticeengines.domain.exposed.pls;

import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.testng.Assert.*;

public class DeleteUsersResultUnitTestNG {
    
    @Test(groups = "unit")
    public void testSerDe() throws IOException {
        ResponseDocument<DeleteUsersResult> doc = new ResponseDocument<>();
        doc.setSuccess(false);

        DeleteUsersResult result = new DeleteUsersResult();
        result.setSuccessUsers(Arrays.asList("sa@c.com", "sb@c.com", "sc@c.com"));
        result.setFailUsers(Arrays.asList("fa@c.com", "fb@c.com", "fc@c.com", "fd@c.com"));
        doc.setResult(result);

        String serializedStr = doc.toString();
        ResponseDocument<DeleteUsersResult> deserializedDoc = ResponseDocument.generateFromJSON(serializedStr, DeleteUsersResult.class);
        
        assertFalse(deserializedDoc.isSuccess());
        assertNull(deserializedDoc.getErrors());

        assertEquals(deserializedDoc.getResult().getSuccessUsers().size(), 3);
        assertEquals(deserializedDoc.getResult().getFailUsers().size(), 4);

    }
}
