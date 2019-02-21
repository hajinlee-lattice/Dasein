package com.latticeengines.modeling.workflow.steps;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.CreatePrematchEventTableReportConfiguration;

public class ValidatePrematchEventTableUnitTestNG {

    private ValidatePrematchEventTable validatePrematchEventTable;

    @Mock
    CreatePrematchEventTableReportConfiguration configuration;

    private ObjectNode json;

    @BeforeClass(groups = "unit")
    public void setup() {
        validatePrematchEventTable = new ValidatePrematchEventTable();
        MockitoAnnotations.initMocks(this);
        org.mockito.Mockito.when(configuration.getMinRows()).thenReturn(300l);
        org.mockito.Mockito.when(configuration.getMinNegativeEvents()).thenReturn(250l);
        org.mockito.Mockito.when(configuration.getMinPositiveEvents()).thenReturn(50l);
        ObjectMapper mapper = new ObjectMapper();
        json = mapper.createObjectNode();
        json.put("count", 298l);
        json.put("events", 49l);
    }

    @Test(groups = "unit")
    public void testExecute() {
        boolean exceptionThrown = false;
        try {
            validatePrematchEventTable.validate(json, configuration);
        } catch (Exception e) {
            exceptionThrown = true;
            assertTrue(e instanceof LedpException);
            assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_32000);
            assertTrue(e.getMessage().contains("domains") && e.getMessage().contains("positive")
                    && e.getMessage().contains("negative"));
        }
        assertTrue(exceptionThrown);
    }

}
