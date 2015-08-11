package com.latticeengines.domain.exposed.pls;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;

public class SimpleBooleanResponseUnitTestNG {
    
    @Test(groups = "unit")
    public void testSerDe() throws IOException {
        ResponseDocument<?> response = SimpleBooleanResponse.successResponse();
        
        String serializedStr = JsonUtils.serialize(response);
        
        ResponseDocument<?> deserializedResponse = JsonUtils.deserialize(serializedStr, ResponseDocument.class);
        assertTrue(deserializedResponse.isSuccess());

        response = SimpleBooleanResponse.failedResponse(Arrays.asList("You know why!"));
        serializedStr = JsonUtils.serialize(response);
        deserializedResponse = JsonUtils.deserialize(serializedStr, ResponseDocument.class);
        assertFalse(deserializedResponse.isSuccess());
        assertEquals(deserializedResponse.getErrors().size(), 1);
        assertEquals(deserializedResponse.getErrors().get(0), "You know why!");
    }
}
