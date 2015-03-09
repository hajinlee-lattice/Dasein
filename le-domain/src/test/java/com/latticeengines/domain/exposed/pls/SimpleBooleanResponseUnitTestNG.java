package com.latticeengines.domain.exposed.pls;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;

import org.testng.annotations.Test;

public class SimpleBooleanResponseUnitTestNG {
    
    @Test(groups = "unit")
    public void testSerDe() throws IOException {
        ResponseDocument<?> response = SimpleBooleanResponse.getSuccessResponse();
        assertTrue(response.isSuccess());

        response = SimpleBooleanResponse.getFailResponse(Arrays.asList("You know why!"));
        assertFalse(response.isSuccess());
        assertEquals(response.getErrors().size(), 1);
        assertEquals(response.getErrors().get(0), "You know why!");
    }
}
