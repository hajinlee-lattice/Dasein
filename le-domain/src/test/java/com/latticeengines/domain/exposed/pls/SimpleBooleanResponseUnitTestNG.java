package com.latticeengines.domain.exposed.pls;

import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.testng.Assert.*;

public class SimpleBooleanResponseUnitTestNG {
    
    @Test(groups = "unit")
    public void testSerDe() throws IOException {
        ResponseDocument response = SimpleBooleanResponse.getSuccessResponse();
        assertTrue(response.isSuccess());

        response = SimpleBooleanResponse.getFailResponse(Arrays.asList("You know why!"));
        assertFalse(response.isSuccess());
        assertEquals(response.getErrors().size(), 1);
        assertEquals(response.getErrors().get(0), "You know why!");
    }
}
