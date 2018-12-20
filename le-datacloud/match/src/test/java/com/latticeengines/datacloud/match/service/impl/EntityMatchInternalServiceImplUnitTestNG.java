package com.latticeengines.datacloud.match.service.impl;

import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityRawSeed;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Set;

import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DC_GOOGLE_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DC_NETFLIX_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DUNS_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DUNS_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DUNS_3;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DUNS_4;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.DUNS_5;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.ELOQUA_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.ELOQUA_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.ELOQUA_3;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.MKTO_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.MKTO_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.MKTO_3;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.NC_FACEBOOK_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.NC_FACEBOOK_2;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.NC_FACEBOOK_3;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.SFDC_1;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.SFDC_4;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.LookupEntry.SFDC_5;
import static com.latticeengines.datacloud.match.testframework.TestEntityMatchUtils.newSeed;

public class EntityMatchInternalServiceImplUnitTestNG {
    private static final String TGEFA_SEED_ID = "testGetLookupEntriesFailedToAssociate";

    @Test(groups = "unit", dataProvider = "getLookupEntriesFailedToAssociate")
    private void testGetLookupEntriesFailedToAssociate(
            @NotNull EntityRawSeed currentState, @NotNull EntityRawSeed seedToAssociate,
            @NotNull Set<EntityLookupEntry> expectedLookupEntries) {
        Assert.assertNotNull(currentState);
        Assert.assertNotNull(seedToAssociate);
        Assert.assertNotNull(expectedLookupEntries);
        EntityMatchInternalServiceImpl service = newService();
        Assert.assertNotNull(service);

        Map<Pair<EntityLookupEntry.Type, String>, Set<String>> existingLookupPairs =
                service.getExistingLookupPairs(currentState);
        Assert.assertNotNull(existingLookupPairs);
        Set<EntityLookupEntry> result = service.getLookupEntriesFailedToAssociate(existingLookupPairs, seedToAssociate);
        Assert.assertEquals(result, expectedLookupEntries);
    }

    @DataProvider(name = "getLookupEntriesFailedToAssociate")
    private Object[][] provideGetLookupEntriesFailedToAssociateTestData() {
        return new Object[][] {
                /*
                 * Case #1: no entries failed to associate
                 */
                {
                        newSeed(TGEFA_SEED_ID, new EntityLookupEntry[0]),
                        newSeed(TGEFA_SEED_ID, NC_FACEBOOK_1, NC_FACEBOOK_3, DC_GOOGLE_1, SFDC_1, MKTO_1),
                        Sets.newHashSet()
                },
                {
                        newSeed(TGEFA_SEED_ID, NC_FACEBOOK_1, NC_FACEBOOK_2, DUNS_1),
                        newSeed(TGEFA_SEED_ID, NC_FACEBOOK_1, NC_FACEBOOK_3, DC_GOOGLE_1, SFDC_1, MKTO_1),
                        Sets.newHashSet()
                },
                {
                        newSeed(TGEFA_SEED_ID, NC_FACEBOOK_1, SFDC_5, MKTO_3, DUNS_1),
                        newSeed(TGEFA_SEED_ID, DC_NETFLIX_2, NC_FACEBOOK_3, NC_FACEBOOK_1, ELOQUA_3),
                        Sets.newHashSet()
                },
                {
                        newSeed(TGEFA_SEED_ID, NC_FACEBOOK_1, SFDC_4, MKTO_2, ELOQUA_2),
                        newSeed(TGEFA_SEED_ID, DC_NETFLIX_2, NC_FACEBOOK_3, NC_FACEBOOK_1, DUNS_5),
                        Sets.newHashSet()
                },
                {
                        // update entries with one to one mapping with the same value is fine
                        newSeed(TGEFA_SEED_ID, NC_FACEBOOK_1, SFDC_4, MKTO_2, ELOQUA_2),
                        newSeed(TGEFA_SEED_ID, NC_FACEBOOK_1, SFDC_4, MKTO_2, ELOQUA_2, DUNS_4),
                        Sets.newHashSet()
                },
                {
                        // update entries with x to one mapping with the same value is fine
                        newSeed(TGEFA_SEED_ID, DUNS_4),
                        newSeed(TGEFA_SEED_ID, NC_FACEBOOK_1, SFDC_4, MKTO_2, ELOQUA_2, DUNS_4),
                        Sets.newHashSet()
                },
                /*
                 * Case #2: conflict in DUNS
                 */
                {
                        newSeed(TGEFA_SEED_ID, NC_FACEBOOK_1, SFDC_4, MKTO_2, ELOQUA_2, DUNS_3),
                        newSeed(TGEFA_SEED_ID, NC_FACEBOOK_1, SFDC_4, MKTO_2, ELOQUA_2, DUNS_4),
                        Sets.newHashSet(DUNS_4)
                },
                {
                        newSeed(TGEFA_SEED_ID, NC_FACEBOOK_1, NC_FACEBOOK_2, DUNS_1),
                        newSeed(TGEFA_SEED_ID, NC_FACEBOOK_1, NC_FACEBOOK_3, DC_GOOGLE_1, SFDC_1, MKTO_1, DUNS_3),
                        Sets.newHashSet(DUNS_3)
                },
                {
                        newSeed(TGEFA_SEED_ID, NC_FACEBOOK_1, SFDC_4, MKTO_2, ELOQUA_2, DUNS_2),
                        newSeed(TGEFA_SEED_ID, DC_NETFLIX_2, NC_FACEBOOK_3, NC_FACEBOOK_1, DUNS_5),
                        Sets.newHashSet(DUNS_5)
                },
                /*
                 * Case #3: conflict in external systems
                 */
                {
                        newSeed(TGEFA_SEED_ID, NC_FACEBOOK_1, SFDC_4, MKTO_2, ELOQUA_2, DUNS_3),
                        newSeed(TGEFA_SEED_ID, MKTO_1, SFDC_4, ELOQUA_1), // no conflict in SFDC_4
                        Sets.newHashSet(MKTO_1, ELOQUA_1)
                },
                {
                        newSeed(TGEFA_SEED_ID, SFDC_5),
                        newSeed(TGEFA_SEED_ID, NC_FACEBOOK_1, NC_FACEBOOK_3, DC_GOOGLE_1, SFDC_1, MKTO_1),
                        Sets.newHashSet(SFDC_1)
                },
                /*
                 * Case #4: conflict in both DUNS & external systems
                 */
                {
                        newSeed(TGEFA_SEED_ID, NC_FACEBOOK_1, SFDC_4, MKTO_2, ELOQUA_2, DUNS_3),
                        newSeed(TGEFA_SEED_ID, MKTO_1, SFDC_4, ELOQUA_1, DUNS_5), // no conflict in SFDC_4
                        Sets.newHashSet(MKTO_1, ELOQUA_1, DUNS_5)
                },
        };
    }

    /*
     * all dependent services to null
     */
    private EntityMatchInternalServiceImpl newService() {
        return new EntityMatchInternalServiceImpl(null, null, null);
    }
}
