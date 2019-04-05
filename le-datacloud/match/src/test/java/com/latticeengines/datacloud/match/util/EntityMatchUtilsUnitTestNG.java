package com.latticeengines.datacloud.match.util;

import static com.latticeengines.domain.exposed.datacloud.match.OperationalMode.CDL_LOOKUP;
import static com.latticeengines.domain.exposed.datacloud.match.OperationalMode.ENTITY_MATCH;
import static com.latticeengines.domain.exposed.datacloud.match.OperationalMode.LDC_MATCH;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Contact;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Transaction;

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

    @Test(groups = "unit", dataProvider = "shouldOutputNewEntity")
    private void testShouldOutputNewEntity(MatchInput input, String currentEntity, boolean expectedResult) {
        boolean result = EntityMatchUtils.shouldOutputNewEntity(input, currentEntity);
        Assert.assertEquals(result, expectedResult);
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

    @DataProvider(name = "shouldOutputNewEntity")
    private Object[][] shouldOutputNewEntityTestData() {
        return new Object[][] { //
                // invalid match input
                { null, Account.name(), false }, //
                { null, Contact.name(), false }, //
                { newEntityMatchInput(null), Account.name(), false }, //
                { newEntityMatchInput(null), Contact.name(), false }, //
                // for account match, no need to output newly allocated entity
                // TODO add case to make sure we do not output new account for account match
                { newEntityMatchInput(Account.name()), Contact.name(), false }, //
                { newEntityMatchInput(Account.name()), Transaction.name(), false }, //
                // for contact match, only output newly created account
                { newEntityMatchInput(Contact.name()), Account.name(), true }, //
                { newEntityMatchInput(Contact.name()), Contact.name(), false }, //
                { newEntityMatchInput(Contact.name()), Transaction.name(), false }, //
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

    /*
     * Helper for testing which new entity to output under current match entity
     */
    private MatchInput newEntityMatchInput(String entity) {
        MatchInput input = new MatchInput();
        input.setOperationalMode(ENTITY_MATCH);
        input.setAllocateId(true);
        input.setOutputNewEntities(true);
        input.setTargetEntity(entity);
        return input;
    }

    private MatchInput newMatchInput(OperationalMode mode, boolean isAllocateId, boolean outputNewEntities) {
        MatchInput input = new MatchInput();
        input.setOperationalMode(mode);
        input.setAllocateId(isAllocateId);
        input.setOutputNewEntities(outputNewEntities);
        input.setTargetEntity(Contact.name());
        return input;
    }
}
