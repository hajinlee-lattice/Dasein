package com.latticeengines.domain.exposed.pls;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;

import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

public class SimpleBooleanResponseUnitTestNG {
    
    @Test(groups = "unit")
    public void testSerDe() throws IOException {
        ResponseDocument<?> response = SimpleBooleanResponse.getSuccessResponse();
        
        String serializedStr = JsonUtils.serialize(response);
        
        ResponseDocument<?> deserializedResponse = JsonUtils.deserialize(serializedStr, ResponseDocument.class);
        assertTrue(deserializedResponse.isSuccess());

        response = SimpleBooleanResponse.getFailResponse(Arrays.asList("You know why!"));
        serializedStr = JsonUtils.serialize(response);
        deserializedResponse = JsonUtils.deserialize(serializedStr, ResponseDocument.class);
        assertFalse(deserializedResponse.isSuccess());
        assertEquals(deserializedResponse.getErrors().size(), 1);
        assertEquals(deserializedResponse.getErrors().get(0), "You know why!");
    }
}
