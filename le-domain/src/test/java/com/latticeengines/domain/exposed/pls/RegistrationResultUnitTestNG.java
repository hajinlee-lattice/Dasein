package com.latticeengines.domain.exposed.pls;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;

import com.latticeengines.domain.exposed.ResponseDocument;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.security.User;

public class RegistrationResultUnitTestNG {
    
    @Test(groups = "unit")
    public void testSerDe() throws IOException {
        ResponseDocument<RegistrationResult> regDoc = new ResponseDocument<>();
        regDoc.setSuccess(true);

        RegistrationResult obj = new RegistrationResult();
        obj.setPassword("password");
        obj.setValid(true);
        User user = new User();
        user.setFirstName("Henry");
        user.setLastName("Ford");
        obj.setConflictingUser(user);
        regDoc.setResult(obj);
        
        String serializedStr = regDoc.toString();
        ResponseDocument<RegistrationResult> deserializedDoc = ResponseDocument.generateFromJSON(serializedStr, RegistrationResult.class);
        
        assertTrue(deserializedDoc.isSuccess());
        assertNull(deserializedDoc.getErrors());

        assertEquals(deserializedDoc.getResult().getPassword(), "password");
        assertTrue(deserializedDoc.getResult().isValid());
        assertEquals(deserializedDoc.getResult().getConflictingUser().getFirstName(), "Henry");
        assertEquals(deserializedDoc.getResult().getConflictingUser().getLastName(), "Ford");
    }
}
