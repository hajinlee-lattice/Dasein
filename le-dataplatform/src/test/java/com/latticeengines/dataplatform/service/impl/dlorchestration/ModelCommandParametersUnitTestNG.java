package com.latticeengines.dataplatform.service.impl.dlorchestration;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.Test;

import com.latticeengines.dataplatform.exposed.service.impl.ModelingServiceTestUtils;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommand;
import com.latticeengines.domain.exposed.dataplatform.dlorchestration.ModelCommandParameter;
import com.latticeengines.domain.exposed.exception.LedpException;

public class ModelCommandParametersUnitTestNG {

    public ModelCommandParameters createModelCommandParameters() {
        return new ModelCommandParameters(ModelingServiceTestUtils.createModelCommandWithCommandParameters(1L)
                .getCommandParameters());
    }

    @Test(groups = "unit", expectedExceptions = LedpException.class)
    public void testInvalidCommandParameters() throws Exception {
        try {
            new ModelCommandParameters(new ArrayList<ModelCommandParameter>());
        } catch (LedpException e) {
            String msg = e.getMessage();
            assertTrue(msg.contains(ModelCommandParameters.KEY_COLS));
            assertTrue(msg.contains(ModelCommandParameters.MODEL_NAME));
            assertTrue(msg.contains(ModelCommandParameters.MODEL_TARGETS));
            assertTrue(msg.contains(ModelCommandParameters.EXCLUDE_COLUMNS));
            assertTrue(msg.contains(ModelCommandParameters.DL_URL));
            assertTrue(msg.contains(ModelCommandParameters.DL_TENANT));
            assertTrue(msg.contains(ModelCommandParameters.DL_QUERY));
            throw e;
        }
    }

    @Test(groups = "unit")
    public void testSplit() {
        List<String> splits = createModelCommandParameters().splitCommaSeparatedStringToList("one, two,  three, four");
        assertEquals(4, splits.size());
        assertEquals("one", splits.get(0));
        assertEquals("two", splits.get(1));
        assertEquals("three", splits.get(2));
        assertEquals("four", splits.get(3));
    }

    @Test(groups = "unit")
    public void testGetEventColumnWithoutReadoutParams() {
        ModelCommand modelCommand = ModelingServiceTestUtils.createModelCommandWithCommandParameters(1L);
        ModelCommandParameters commandParameters = new ModelCommandParameters(modelCommand.getCommandParameters());
        assertEquals(commandParameters.getEventColumnName(), "P1_Event");
    }

    @Test(groups = "unit")
    public void testGetEventColumnWithReadoutParams() {
        ModelCommand modelCommand = ModelingServiceTestUtils.createModelCommandWithFewRowsAndReadoutTargets(1L,
                "ModelCommandCallableTestNG_eventtable_fewrows", false, true);
        ModelCommandParameters commandParameters = new ModelCommandParameters(modelCommand.getCommandParameters());
        assertEquals(commandParameters.getEventColumnName(), "CATEGORY");
    }

}
