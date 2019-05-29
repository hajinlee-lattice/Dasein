package com.latticeengines.datacloud.match.util;

import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.newSeed;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DC_FACEBOOK_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DC_GOOGLE_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DC_GOOGLE_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DUNS_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DUNS_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DUNS_3;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DUNS_5;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.MKTO_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.MKTO_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.NC_NETFLIX_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.SFDC_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.SFDC_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.SFDC_4;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.SFDC_5;
import static com.latticeengines.domain.exposed.datacloud.match.OperationalMode.CDL_LOOKUP;
import static com.latticeengines.domain.exposed.datacloud.match.OperationalMode.ENTITY_MATCH;
import static com.latticeengines.domain.exposed.datacloud.match.OperationalMode.LDC_MATCH;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Country;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Domain;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.LatticeAccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Name;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.State;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Website;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Account;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Contact;
import static com.latticeengines.domain.exposed.query.BusinessEntity.Transaction;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityRawSeed;

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

    @Test(groups = "unit", dataProvider = "shouldOverrideAttribute")
    private void testShouldOverrideAttribute(String entity, String attrName, boolean expectedResult) {
        boolean result = EntityMatchUtils.shouldOverrideAttribute(entity, attrName);
        Assert.assertEquals(result, expectedResult,
                String.format("shouldOverrideAttribute does not match the expected result for entity=%s, attrName=%s",
                        entity, attrName));
    }

    @Test(groups = "unit", dataProvider = "mergeSeed")
    private void testMergeSeed(EntityRawSeed base, EntityRawSeed change, EntityRawSeed expectedResult,
            EntityLookupEntry... conflictEntries) {
        Set<EntityLookupEntry> conflicts = Arrays.stream(conflictEntries).collect(Collectors.toSet());
        EntityRawSeed result = EntityMatchUtils.mergeSeed(base, change, conflicts);
        Assert.assertNotNull(result, "Merged seed should not be null");
        Assert.assertTrue(TestEntityMatchUtils.equalsDisregardPriority(result, expectedResult), String.format(
                "Merged seed (%s) should be the same as the expected merged seed (%s)", result, expectedResult));
    }

    @DataProvider(name = "mergeSeed")
    private Object[][] mergeSeedTestData() {
        return new Object[][] { //
                /*
                 * change seed is null
                 */
                { newSeed("tc_1", SFDC_1, DC_GOOGLE_1), null, newSeed("tc_1", SFDC_1, DC_GOOGLE_1),
                        new EntityLookupEntry[0] },
                { newSeed("tc_2", LatticeAccountId.name(), "ldc_id_1"), null,
                        newSeed("tc_2", LatticeAccountId.name(), "ldc_id_1"), new EntityLookupEntry[0] },
                /*
                 * update with no conflicts (many to many or x to one with the same values)
                 */
                { newSeed("tc_3", SFDC_1, DC_GOOGLE_1), newSeed("tc_3", MKTO_1, DC_GOOGLE_1, DC_GOOGLE_2, DUNS_3),
                        newSeed("tc_3", SFDC_1, MKTO_1, DC_GOOGLE_1, DC_GOOGLE_2, DUNS_3), new EntityLookupEntry[0] },
                { newSeed("tc_4", MKTO_2, NC_NETFLIX_1), newSeed("tc_4", MKTO_2, DC_GOOGLE_1, SFDC_2),
                        newSeed("tc_4", MKTO_2, DC_GOOGLE_1, SFDC_2, NC_NETFLIX_1), new EntityLookupEntry[0] },
                { newSeed("tc_5", DUNS_2), newSeed("tc_5", DUNS_2, DC_GOOGLE_1, SFDC_2),
                        newSeed("tc_5", DUNS_2, DC_GOOGLE_1, SFDC_2), new EntityLookupEntry[0] },
                { newSeed("tc_6", MKTO_1, DUNS_5, DC_FACEBOOK_2), newSeed("tc_6", MKTO_1, DUNS_5, DC_FACEBOOK_2),
                        newSeed("tc_6", MKTO_1, DUNS_5, DC_FACEBOOK_2), new EntityLookupEntry[0] },
                /*
                 * update with conflicts (conflict entries will not be merged)
                 */
                { newSeed("tc_7", MKTO_1, DUNS_5, DC_FACEBOOK_2), newSeed("tc_7", MKTO_2, DUNS_1),
                        newSeed("tc_7", MKTO_1, DUNS_5, DC_FACEBOOK_2), new EntityLookupEntry[0] },
                { newSeed("tc_8", MKTO_2, SFDC_4), newSeed("tc_8", MKTO_1, SFDC_5, DUNS_1),
                        newSeed("tc_8", MKTO_2, SFDC_4, DUNS_1), new EntityLookupEntry[0] },
                /*
                 * explicitly specify conflicts that will not be merged
                 */
                { newSeed("tc_9", SFDC_1, DC_GOOGLE_1), newSeed("tc_9", MKTO_1, DC_GOOGLE_1, DC_GOOGLE_2, DUNS_3),
                        newSeed("tc_9", SFDC_1, MKTO_1, DC_GOOGLE_1, DUNS_3), new EntityLookupEntry[] { DC_GOOGLE_2 } },
                // note that MKTO_2 still exist in the merged seed even though specify as
                // conflict somehow as it is in the base seed
                { newSeed("tc_10", MKTO_2, NC_NETFLIX_1), newSeed("tc_10", MKTO_2, DC_GOOGLE_1, SFDC_2),
                        newSeed("tc_10", MKTO_2, NC_NETFLIX_1),
                        new EntityLookupEntry[] { SFDC_2, DC_GOOGLE_1, MKTO_2 } },
                /*
                 * merge attributes
                 */
                // LatticeAccountId should not update with account match, others should update
                { attrAccountSeed("tc_11", "ldc_id_1", null, null), attrAccountSeed("tc_11", "ldc_id_2", null, null),
                        attrAccountSeed("tc_11", "ldc_id_1", null, null), new EntityLookupEntry[0] },
                { attrAccountSeed("tc_12", "ldc_id_1", "google.com", "USA"),
                        attrAccountSeed("tc_12", "ldc_id_2", "facebook.com", "CN"),
                        attrAccountSeed("tc_12", "ldc_id_1", "facebook.com", "CN"), new EntityLookupEntry[0] }, };
    }

    @DataProvider(name = "shouldOverrideAttribute")
    private Object[][] shouldOverrideAttributeTestData() {
        return new Object[][] { //
                // first win attributes in account match. currently only lattice account id
                { Account.name(), LatticeAccountId.name(), false }, //
                { Account.name(), attr(LatticeAccountId.name()), false }, //
                // last win attributes in account match
                { Account.name(), AccountId.name(), true }, //
                { Account.name(), attr(AccountId.name()), true }, //
                { Account.name(), Name.name(), true }, //
                { Account.name(), attr(Name.name()), true }, //
                { Account.name(), Country.name(), true }, //
                { Account.name(), attr(Country.name()), true }, //
                { Account.name(), Domain.name(), true }, //
                { Account.name(), attr(Domain.name()), true }, //
                { Account.name(), State.name(), true }, //
                { Account.name(), attr(State.name()), true }, //
                // last win attributes in contact match
                { Contact.name(), LatticeAccountId.name(), true }, //
                { Contact.name(), attr(LatticeAccountId.name()), true }, //
                { Contact.name(), Name.name(), true }, //
                { Contact.name(), attr(Name.name()), true }, //
                { Contact.name(), Country.name(), true }, //
                { Contact.name(), attr(Country.name()), true }, //
        };
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
                // for account match, still output new accounts for transaction match
                { newEntityMatchInput(Account.name()), Account.name(), true }, //
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

    private String attr(String attrName) {
        return DataCloudConstants.ENTITY_PREFIX_SEED_ATTRIBUTES + attrName;
    }

    /*
     * create an account seed with standard attributes
     */
    private EntityRawSeed attrAccountSeed(@NotNull String entityId, String latticeAccountId, String website,
            String country) {
        Map<String, String> attrs = new HashMap<>();
        if (latticeAccountId != null) {
            attrs.put(LatticeAccountId.name(), latticeAccountId);
        }
        if (website != null) {
            attrs.put(Website.name(), website);
        }
        if (country != null) {
            attrs.put(Country.name(), country);
        }
        return new EntityRawSeed(entityId, Account.name(), Collections.emptyList(), attrs);
    }
}
