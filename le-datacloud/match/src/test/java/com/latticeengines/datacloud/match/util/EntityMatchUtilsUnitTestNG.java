package com.latticeengines.datacloud.match.util;

import static com.latticeengines.domain.exposed.datacloud.match.OperationalMode.CDL_LOOKUP;
import static com.latticeengines.domain.exposed.datacloud.match.OperationalMode.ENTITY_MATCH;
import static com.latticeengines.domain.exposed.datacloud.match.OperationalMode.LDC_MATCH;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;

public class EntityMatchUtilsUnitTestNG {

    @Test(groups = "unit", dataProvider = "shouldOuputNewEntities")
    private void testShouldOutputNewEntities(MatchInput input, boolean expectedResult) {
        boolean result = EntityMatchUtils.shouldOutputNewEntities(input);
        Assert.assertEquals(result, expectedResult, errorMsg(input, expectedResult));
    }

    @DataProvider(name = "shouldOuputNewEntities")
    private Object[][] shouldOuputNewEntitiesTestData() {
        // currently, only output new entities if (a) is entity match, (b) is allocateId
        // mode and (c) outputNewEntities flag is true
        return new Object[][] { //
                // non entity match
                { null, false }, //
                { newMatchInput(null, true, true), false }, //
                { newMatchInput(null, false, false), false }, //
                { newMatchInput(null, true, false), false }, //
                { newMatchInput(null, false, true), false }, //
                { newMatchInput(LDC_MATCH, false, false), false }, //
                { newMatchInput(LDC_MATCH, true, true), false }, //
                { newMatchInput(LDC_MATCH, true, false), false }, //
                { newMatchInput(LDC_MATCH, false, true), false }, //
                { newMatchInput(CDL_LOOKUP, true, true), false }, //
                { newMatchInput(CDL_LOOKUP, false, false), false }, //
                { newMatchInput(CDL_LOOKUP, true, false), false }, //
                { newMatchInput(CDL_LOOKUP, false, true), false }, //

                // entity match
                { newMatchInput(ENTITY_MATCH, false, false), false }, //
                { newMatchInput(ENTITY_MATCH, false, true), false }, //
                { newMatchInput(ENTITY_MATCH, true, false), false }, //
                { newMatchInput(ENTITY_MATCH, true, true), true }, //
        };
    }

    private String errorMsg(MatchInput input, boolean expectedResult) {
        return String.format("ExpectedResult=%b for input %s", expectedResult, debugString(input));
    }

    private String debugString(MatchInput input) {
        if (input == null) {
            return null;
        }
        return String.format("MatchInput{OperationalMode=%s,isAllocateId=%b,outputNewEntities=%b}",
                input.getOperationalMode(), input.isAllocateId(), input.isOutputNewEntities());
    }

    private MatchInput newMatchInput(OperationalMode mode, boolean isAllocateId, boolean outputNewEntities) {
        MatchInput input = new MatchInput();
        input.setOperationalMode(mode);
        input.setAllocateId(isAllocateId);
        input.setOutputNewEntities(outputNewEntities);
        return input;
    }
}
