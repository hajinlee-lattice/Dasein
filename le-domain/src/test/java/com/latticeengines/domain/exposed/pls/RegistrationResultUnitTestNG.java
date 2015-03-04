package com.latticeengines.domain.exposed.pls;

import com.latticeengines.domain.exposed.security.User;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

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
        obj.setUser(user);
        regDoc.setResult(obj);
        
        String serializedStr = regDoc.toString();
        ResponseDocument<RegistrationResult> deserializedDoc = ResponseDocument.generateFromJSON(serializedStr, RegistrationResult.class);
        
        assertTrue(deserializedDoc.isSuccess());
        assertNull(deserializedDoc.getErrors());

        assertEquals(deserializedDoc.getResult().getPassword(), "password");
        assertTrue(deserializedDoc.getResult().isValid());
        assertEquals(deserializedDoc.getResult().getUser().getFirstName(), "Henry");
        assertEquals(deserializedDoc.getResult().getUser().getLastName(), "Ford");
    }
}
