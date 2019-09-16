package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment.SERVING;
import static com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment.STAGING;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.testframework.EntityMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput.EntityKeyMap;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.testframework.service.impl.SimpleRetryAnalyzer;
import com.latticeengines.testframework.service.impl.SimpleRetryListener;

/**
 * This test is mainly focused on Account match with AllocateId mode
 */
@Listeners({ SimpleRetryListener.class })
public class AccountMatchCorrectnessTestNG extends EntityMatchFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(AccountMatchCorrectnessTestNG.class);

    /*****************************************************************************
     * ATTENTION:
     * 1. Please DO NOT re-format comments in this class
     * 2. Use different tenant for each test/test case to support parallel execution
     *****************************************************************************/

    /**
     * <pre>
     * [ CustomerAccountId, MKTO, ELOQUA, Name, Domain, Country, State ]
     * </pre>
     */
    private static final String ID_ACCT = "CustomerAccountId";
    private static final String ID_MKTO = "MKTO";
    private static final String ID_ELOQUA = "ELOQUA";

    private static final String[] SYSTEM_ID_FIELDS = new String[] { ID_ACCT, ID_MKTO, ID_ELOQUA };
    private static final MatchKey[] MATCH_KEY_FIELDS = new MatchKey[] { MatchKey.Name, MatchKey.Domain,
            MatchKey.Country, MatchKey.State, MatchKey.DUNS, MatchKey.PreferredEntityId };
    private static final String[] NON_SYSTEM_ID_FIELDS = Arrays.stream(MATCH_KEY_FIELDS).map(MatchKey::name)
            .toArray(String[]::new);
    private static final List<String> FIELDS = Arrays.asList(ArrayUtils.addAll(SYSTEM_ID_FIELDS, NON_SYSTEM_ID_FIELDS));
    // Fields for multi-domain matching.
    private static final String[] WEBSITE_EMAIL_FIELDS = new String[] {
            MatchKey.Name.toString(), "Website", MatchKey.Country.toString(), MatchKey.State.toString(),
            MatchKey.DUNS.toString(), MatchKey.Email.toString() };
    private static final String[] FOUR_DOMAIN_FIELDS = new String[] {
            MatchKey.Name.toString(), "Domain1", MatchKey.Country.toString(), MatchKey.State.toString(), "Email2",
            MatchKey.DUNS.toString(), "Domain2", "Email1" };
    private static final List<String> FIELDS_WITH_EMAIL = Arrays.asList(ArrayUtils.addAll(SYSTEM_ID_FIELDS,
            WEBSITE_EMAIL_FIELDS));
    private static final List<String> FIELDS_WITH_FOUR_DOMAINS = Arrays.asList(ArrayUtils.addAll(SYSTEM_ID_FIELDS,
            FOUR_DOMAIN_FIELDS));

    /**
     * Define 5 Account MatchKey groups:
     * ID_ACCT, ID_MKTO, DUNS, Domain, Name
     *
     * MatchKey Group -> Actual MatchKey mapping:
     * ID_ACCT -> [ID_ACCT]
     * ID_MKTO -> [ID_MKTO]
     * DUNS -> [Customer DUNS] or [Name + Country] or [Domain +Country]
     * NOTE: Choose above 3 options for DUNS group due to DUNS in Account match is from LDC match.
     *       These 3 are different paths in LDC match to get DUNS
     *       Customer DUNS: DUNS is from customer, then confirm existence in AMLookup
     *       Name + Country: DUNS is from DnB, then confirm existence in AMLookup
     *       Domain + Country: DUNS is from AMLookup
     *       Each path only select 1 set of MatchKeys because this test is focused on Account match, not LDC match
     * Domain -> [Domain + Country] or [Domain]
     * Name -> [Name + Country] or [Name]
     * NOTE: Be cautious to add more key groups / mapped keys. It could make some test case sets too large
     **/
    private static final Map<String, List<List<String>>> KEY_GROUP_MAP = new HashMap<>();
    static {
        KEY_GROUP_MAP.put(ID_ACCT, Arrays.asList(Arrays.asList(ID_ACCT)));
        KEY_GROUP_MAP.put(ID_MKTO, Arrays.asList(Arrays.asList(ID_MKTO)));
        KEY_GROUP_MAP.put(MatchKey.DUNS.name(), //
                Arrays.asList( //
                        Arrays.asList(MatchKey.DUNS.name()), //
                        Arrays.asList(MatchKey.Domain.name(), MatchKey.Country.name()), //
                        Arrays.asList(MatchKey.Name.name(), MatchKey.Country.name())));
        KEY_GROUP_MAP.put(MatchKey.Domain.name(),
                Arrays.asList(Arrays.asList(MatchKey.Domain.name()),
                        Arrays.asList(MatchKey.Domain.name(), MatchKey.Country.name())));
        KEY_GROUP_MAP.put(MatchKey.Name.name(),
                Arrays.asList(Arrays.asList(MatchKey.Name.name()),
                        Arrays.asList(MatchKey.Name.name(), MatchKey.Country.name())));
    }

    // MatchKeys used in Account match decision graph (Don't consider MatchKey
    // combinations in LDC match)
    private static final String[] ACCOUNT_KEYS_PRIORITIZED = { ID_ACCT, ID_MKTO, MatchKey.Domain.name(),
            MatchKey.Name.name(), MatchKey.DUNS.name() };
    // Reduced set of MatchKeys used in Account match decision graph. Only one
    // key per actor to reduce # test case
    private static final String[] ACCOUNT_KEYS_REDUCED = { ID_ACCT, MatchKey.DUNS.name(), MatchKey.Domain.name(),
            MatchKey.Name.name() };
    // Account MatchKey -> Index in FIELDS
    private static final Map<String, Integer> ACCOUNT_KEYIDX_MAP = Stream.of(ACCOUNT_KEYS_PRIORITIZED)
            .collect(Collectors.toMap(key -> key, key -> FIELDS.indexOf(key)));
    // Account MatchKey with many-to-many relationship to Account
    private static final Set<String> ACCOUNT_KEYS_MANY_TO_MANY = new HashSet<>(
            Arrays.asList(MatchKey.Domain.name(), MatchKey.Name.name()));

    @Test(groups = "functional", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testAllocateAndLookup() {
        Tenant tenant = newTestTenant();

        // Data schema: ID_ACCT, ID_MKTO, ID_ELOQUA, Name, Domain, Country,
        // State, DUNS, preferred ID
        // test allocate mode
        List<Object> data = Arrays.asList("acct_1", "mkto_1", null, "GOOGLE", null, "USA", null, null, null);
        MatchOutput output = matchAccount(data, true, tenant, getEntityKeyMap(), FIELDS, null).getRight();
        String entityId = verifyAndGetEntityId(output);

        // publish for testing lookup
        publishToServing(tenant, BusinessEntity.Account);

        // test lookup, make sure we get the correct entity id with each match key
        Assert.assertEquals(lookupAccount(tenant, "acct_1", null, null, null, null, null, null, null), entityId);
        Assert.assertEquals(lookupAccount(tenant, null, "mkto_1", null, null, null, null, null, null), entityId);
        Assert.assertEquals(lookupAccount(tenant, null, null, null, "GOOGLE", null, "USA", null, null), entityId);
    }

    @Test(groups = "functional", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testSpecificServingVersion() {
        Tenant tenant = newTestTenant();

        List<Object> data = Arrays.asList("acct_1", null, null, null, null, null, null, null, null);
        MatchOutput output = matchAccount(data, true, tenant, getEntityKeyMap(), FIELDS, null).getRight();
        String entityId = verifyAndGetEntityId(output);

        // publish for testing lookup
        publishToServing(tenant, BusinessEntity.Account);

        int currentVersion = entityMatchVersionService.getCurrentVersion(SERVING, tenant);
        int nextVersion = entityMatchVersionService.getNextVersion(SERVING, tenant);
        // can lookup the allocated account without specifying version or specify
        // current version explicitly
        Assert.assertEquals(lookupAccount(tenant, null, true, "acct_1", null, null, null, null, null, null, null),
                entityId);
        Assert.assertEquals(
                lookupAccount(tenant, currentVersion, true, "acct_1", null, null, null, null, null, null, null),
                entityId);
        // should not be able to lookup account allocated with current version when
        // specifying next version
        Assert.assertNull(
                lookupAccount(tenant, nextVersion, false, "acct_1", null, null, null, null, null, null, null));

        // clear staging and allocate with next version (empty universe), should get
        // another ID
        testEntityMatchService.bumpVersion(tenant.getId(), STAGING);
        output = matchAccount(data, true, tenant, getEntityKeyMap(), FIELDS, nextVersion).getRight();
        String entityIdInNextVersion = verifyAndGetEntityId(output);
        Assert.assertNotEquals(entityIdInNextVersion, entityId,
                String.format("EntityId in next version %d should not be the same as in current version %d",
                        nextVersion, currentVersion));

        // publish to next serving version
        publishToServing(tenant, nextVersion, BusinessEntity.Account);
        // lookup with current version still the same
        Assert.assertEquals(lookupAccount(tenant, null, true, "acct_1", null, null, null, null, null, null, null),
                entityId);
        Assert.assertEquals(
                lookupAccount(tenant, currentVersion, true, "acct_1", null, null, null, null, null, null, null),
                entityId);
        // lookup with next version will get new ID
        Assert.assertEquals(
                lookupAccount(tenant, nextVersion, true, "acct_1", null, null, null, null, null, null, null),
                entityIdInNextVersion);
    }

    @Test(groups = "functional", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testPreferredIdAllocate() {
        Tenant tenant = newTestTenant();
        String preferredId = "legacy_entity_id";
        String preferredId2 = "legacy_entity_id2";

        List<Object> data = Arrays.asList("acct_1", "mkto_1", null, "GOOGLE", null, "USA", null, null, preferredId);
        MatchOutput output = matchAccount(data, true, tenant, getEntityKeyMap(), FIELDS, null).getRight();
        String entityId = verifyAndGetEntityId(output);
        Assert.assertEquals(entityId, preferredId);

        // no need to allocate since this record match to preferredId
        data = Arrays.asList("acct_1", "mkto_1", null, "GOOGLE", null, "USA", null, null, "some_other_id");
        output = matchAccount(data, true, tenant, getEntityKeyMap(), FIELDS, null).getRight();
        entityId = verifyAndGetEntityId(output);
        Assert.assertEquals(entityId, preferredId);

        data = Arrays.asList("acct_2", "mkto_2", null, "GOOGLE", null, "USA", null, null, preferredId);
        output = matchAccount(data, true, tenant, getEntityKeyMap(), FIELDS, null).getRight();
        String entityId2 = verifyAndGetEntityId(output);
        Assert.assertNotEquals(entityId2, preferredId, "Preferred ID should already be taken by the first account");

        // new record with empty/null preferred ID (system will take over and random)
        data = Arrays.asList("acct_3", "mkto_3", null, "GOOGLE", null, "USA", null, null, "");
        output = matchAccount(data, true, tenant, getEntityKeyMap(), FIELDS, null).getRight();
        entityId = verifyAndGetEntityId(output);
        Assert.assertFalse(entityId.isEmpty());
        Assert.assertNotEquals(entityId, preferredId);
        Assert.assertNotNull(entityId, entityId2);
        data = Arrays.asList("acct_4", "mkto_4", null, "GOOGLE", null, "USA", null, null, null);
        output = matchAccount(data, true, tenant, getEntityKeyMap(), FIELDS, null).getRight();
        verifyAndGetEntityId(output);

        // record has conflict with existing account + unused preferred ID
        data = Arrays.asList("acct_5", "mkto_5", null, "GOOGLE", null, "USA", null, null, preferredId2);
        output = matchAccount(data, true, tenant, getEntityKeyMap(), FIELDS, null).getRight();
        entityId = verifyAndGetEntityId(output);
        Assert.assertEquals(entityId, preferredId2, "Should get the preferred ID since it's not taken yet");
    }

    /*
     * make sure that when many to many match key already map to other entity (and
     * existing data is in serving env), the existing mapping is NOT overridden.
     */
    @Test(groups = "functional", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testDomainConflictFromServing() {
        Tenant tenant = newTestTenant();
        String domain = "google.com";

        List<Object> data1 = Arrays.asList("acct_1", null, null, null, domain, "USA", null, null, null);
        MatchOutput output = matchAccount(data1, true, tenant, getEntityKeyMap(), FIELDS, null).getRight();
        String entityId1 = verifyAndGetEntityId(output);

        publishToServing(tenant, BusinessEntity.Account);
        testEntityMatchService.bumpVersion(tenant.getId(), EntityMatchEnvironment.STAGING);

        // domain should map to account 1
        Assert.assertEquals(lookupAccount(tenant, null, null, null, null, domain, "USA", null, null), entityId1,
                String.format("Domain=%s should match to created account", domain));

        // account ID have diff value in existing account, create new one.
        // domain already used by existing account, and should NOT be mapped to account2
        List<Object> data2 = Arrays.asList("acct_2", null, null, null, "google.com", "USA", null, null, null);
        output = matchAccount(data2, true, tenant, getEntityKeyMap(), FIELDS, null).getRight();
        String entityId2 = verifyAndGetEntityId(output);
        // should get two accounts since account id are different
        Assert.assertNotEquals(entityId1, entityId2);

        publishToServing(tenant, BusinessEntity.Account);
        // bump version and clear cache since the service for lookup/allocate are shared
        testEntityMatchService.bumpVersion(tenant.getId(), EntityMatchEnvironment.STAGING);

        // domain should still map to account 1 (should NOT be overridden by rows import
        // after publish)
        Assert.assertEquals(lookupAccount(tenant, null, null, null, null, domain, "USA", null, null), entityId1,
                String.format("Domain=%s should match to existing account", domain));
        // test account id lookup
        Assert.assertEquals(lookupAccount(tenant, "acct_1", null, null, null, null, null, null, null), entityId1);
        Assert.assertEquals(lookupAccount(tenant, "acct_2", null, null, null, null, null, null, null), entityId2);
    }

    @Test(groups = "functional", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testPublicDomain() {
        Tenant tenant = newTestTenant();

        // Baseline Normal Case
        // Data schema: ID_ACCT, ID_MKTO, ID_ELOQUA, Name, Domain, Country, State, DUNS,
        // preferred ID
        // public domain without DUNS/name, but in email format, treat as public domain
        List<Object> data = Arrays.asList(null, null, null, null, "aaa@gmail.com", "USA", null, null, null);
        MatchOutput output = matchAccount(data, true, tenant, null, FIELDS, null).getRight();
        logAndVerifyMatchLogsAndErrors(FIELDS, data, output,
                Arrays.asList("All the domains are public domain: gmail.com"));
        Assert.assertEquals(verifyAndGetEntityId(output), DataCloudConstants.ENTITY_ANONYMOUS_ID);

        // public domain without DUNS/name, and not in email format, treat as normal domain
        data = Arrays.asList(null, null, null, null, "gmail.com", "USA", null, null, null);
        output = matchAccount(data, true, tenant, null, FIELDS, null).getRight();
        logAndVerifyMatchLogsAndErrors(FIELDS, data, output, null);
        String publicAsNormalDomainEntityId = verifyAndGetEntityId(output);
        Assert.assertNotNull(publicAsNormalDomainEntityId);

        // public domain with name, treat as public domain
        data = Arrays.asList(null, null, null, "public domain company name", "gmail.com", "USA", null, null, null);
        output = matchAccount(data, true, tenant, null, FIELDS, null).getRight();
        logAndVerifyMatchLogsAndErrors(FIELDS, data, output,
                Arrays.asList("All the domains are public domain: gmail.com"));
        String entityId = verifyAndGetEntityId(output);
        Assert.assertNotNull(entityId);
        // Data matched on name + location and finds a match amazingly enough.
        Assert.assertNotEquals(entityId, publicAsNormalDomainEntityId);
        Assert.assertNotEquals(entityId, DataCloudConstants.ENTITY_ANONYMOUS_ID);

        // public domain with (invalid) DUNS, treat as public domain
        data = Arrays.asList(null, null, null, null, "gmail.com", "USA", null, "000000000", null);
        output = matchAccount(data, true, tenant, null, FIELDS, null).getRight();
        logAndVerifyMatchLogsAndErrors(FIELDS, data, output,
                Arrays.asList("All the domains are public domain: gmail.com"));
        entityId = verifyAndGetEntityId(output);
        Assert.assertNotNull(entityId);
        Assert.assertNotEquals(entityId, publicAsNormalDomainEntityId);
        Assert.assertEquals(entityId, DataCloudConstants.ENTITY_ANONYMOUS_ID);

        // public domain, in email format, with PublicDomainAsNormalDomain set
        // true, treat as normal domain
        data = Arrays.asList(null, null, null, null, "aaa@gmail.com", "USA", null, null, null);
        output = matchAccount(data, true, tenant, getEntityKeyMap(), FIELDS, true, null).getRight();
        logAndVerifyMatchLogsAndErrors(FIELDS, data, output, null);
        entityId = verifyAndGetEntityId(output);
        Assert.assertEquals(entityId, publicAsNormalDomainEntityId);
    }

    @Test(groups = "functional", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testMultiDomainKeys() {
        Tenant tenant = newTestTenant();

        //
        // Pre-populate entries in the match table to match against during tests.
        //
        // Setup match entry with domain as lattice-engines.com.
        // Data schema: ID_ACCT, ID_MKTO, ID_ELOQUA, Name, Domain, Country,
        // State, DUNS
        List<Object> data = Arrays.asList(null, null, null, null, "lattice-engines.com", "USA", null, null, null);
        MatchOutput output = matchAccount(data, true, tenant, null, FIELDS, null).getRight();
        logAndVerifyMatchLogsAndErrors(FIELDS, data, output, null);
        String latticeEntityId = verifyAndGetEntityId(output);
        Assert.assertNotNull(latticeEntityId);

        // Setup match entry with domain as marketo.com.
        // Data schema: ID_ACCT, ID_MKTO, ID_ELOQUA, Name, Domain, Country,
        // State, DUNS
        data = Arrays.asList(null, null, null, null, "marketo.com", "USA", null, null, null);
        output = matchAccount(data, true, tenant, null, FIELDS, null).getRight();
        logAndVerifyMatchLogsAndErrors(FIELDS, data, output, null);
        String marketoEntityId = verifyAndGetEntityId(output);
        Assert.assertNotNull(marketoEntityId);
        Assert.assertNotEquals(latticeEntityId, marketoEntityId);

        //
        // Test code functionality prioritizing email over website.
        //
        // Test that email is matched before website.
        // Data schema: ID_ACCT, ID_MKTO, ID_ELOQUA, Name, Website, Country,
        // State, DUNS, Email
        data = Arrays.asList(null, null, null, null, "www.lattice-engines.com", "USA", null, null,
                "private@marketo.com");
        // Create an EntityKeyMap as in other tests.
        EntityKeyMap entityKeyMap = getEntityKeyMap(FIELDS_WITH_EMAIL);
        // Fix the KeyMap for domain to include Email and Website, in that order.
        entityKeyMap.getKeyMap().put(MatchKey.Domain, Arrays.asList("Email", "Website"));
        output = matchAccount(data, true, tenant, entityKeyMap, FIELDS_WITH_EMAIL, null).getRight();
        logAndVerifyMatchLogsAndErrors(FIELDS_WITH_EMAIL, data, output, null);
        String entityId = verifyAndGetEntityId(output);
        Assert.assertEquals(entityId, marketoEntityId);

        // Test that website is matched if email is a public domain.
        data = Arrays.asList(null, null, null, null, "http://www.lattice-engines.com", "USA", null, null,
                "public@gmail.com");
        output = matchAccount(data, true, tenant, entityKeyMap, FIELDS_WITH_EMAIL, null).getRight();
        logAndVerifyMatchLogsAndErrors(FIELDS_WITH_EMAIL, data, output, null);
        entityId = verifyAndGetEntityId(output);
        Assert.assertEquals(entityId, latticeEntityId);

        // Test that no match is found if both email and domain are public domains.
        data = Arrays.asList(null, null, null, null, "public@hotmail.com", "USA", null, null, "public@yahoo.com");
        output = matchAccount(data, true, tenant, entityKeyMap, FIELDS_WITH_EMAIL, null).getRight();
        logAndVerifyMatchLogsAndErrors(FIELDS_WITH_EMAIL, data, output,
                Arrays.asList("All the domains are public domain: hotmail.com,yahoo.com"));
        entityId = verifyAndGetEntityId(output);
        Assert.assertEquals(entityId, DataCloudConstants.ENTITY_ANONYMOUS_ID);

        // Test extreme case with two email and two website domain fields.
        // Data schema: ID_ACCT, ID_MKTO, ID_ELOQUA, Name, Domain1, Country,
        // State, Email2, DUNS, Domain2, Email1
        data = Arrays.asList(null, null, null, null, "www.aol.com", "USA", "CA",
                "private@lattice-engines.com", "000000000", "not_a_domain_or_email", "somebody@outlook.com");

        // Set up match request.  Fix the KeyMap for domain.
        entityKeyMap.getKeyMap().put(MatchKey.Domain, Arrays.asList("Email1", "Domain1", "Domain2", "Email2"));
        output = matchAccount(data, true, tenant, entityKeyMap, FIELDS_WITH_FOUR_DOMAINS, null).getRight();
        logAndVerifyMatchLogsAndErrors(FIELDS_WITH_FOUR_DOMAINS, data, output, null);
        entityId = verifyAndGetEntityId(output);
        Assert.assertEquals(entityId, latticeEntityId);
    }

    /**
     * AC for DUNS keys
     *
     * 1. If customer provided DUNS doesn't exist in LDC, don't use it in
     * Account match. If using other match keys in LDC could find DUNS, use
     * matched LDC_DUNS in Account match instead. If no LDC_DUNS matched, return
     * orphan EntityId
     *
     * 2. For any MatchKeys supported in LDC match, as long as it can match to
     * LatticeAccountId, it should be able to return DUNS of that
     * LatticeAccountId to Account match
     */
    @Test(groups = "functional", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testDunsKey() {
        String tenantId = createNewTenantId();
        Tenant tenant = new Tenant(tenantId);

        // Data schema: ID_ACCT, ID_MKTO, ID_ELOQUA, Name, Domain, Country,
        // State, DUNS, preferred ID
        // Duns in input doesn't exist in LDC and there is no other match key,
        // return orphan EntityId
        List<Object> data = Arrays.asList(null, null, null, null, null, null, null, "000000000", null);
        MatchOutput output = matchAccount(data, true, tenant, null, FIELDS, null).getRight();
        Assert.assertEquals(verifyAndGetEntityId(output), DataCloudConstants.ENTITY_ANONYMOUS_ID);

        // If customer provided DUNS doesn't exist in LDC, don't use it in
        // Account match. If using other match keys in LDC could find DUNS, use
        // matched LDC_DUNS in Account match instead
        testEntityMatchService.bumpVersion(tenantId);
        List<List<Object>> dataList = Arrays.asList( //
                // google duns
                Arrays.asList(null, null, null, null, null, null, null, "060902413", null), //
                // google name + location
                Arrays.asList(null, null, null, "google", null, "usa", "ca", null, null), //
                // google name + location + customer's non-existing duns
                Arrays.asList(null, null, null, "google", null, "usa", "ca", "000000000", null), //
                // for domain based match, ldc match has these combinations of
                // lookup: domain only, domain + country + zipcode (not tested
                // here), domain + country + state, domain + country
                // google domain + country + state
                Arrays.asList(null, null, null, null, "google.com", "usa", "ca", null, null), //
                // google domain + country + state + customer's non-existing
                // duns
                Arrays.asList(null, null, null, null, "google.com", "usa", "ca", "000000000", null), //
                // google domain + country
                Arrays.asList(null, null, null, null, "google.com", "usa", null, null, null), //
                // google domain + country + customer's non-existing duns
                Arrays.asList(null, null, null, null, "google.com", "usa", null, "000000000", null), //
                // google domain
                Arrays.asList(null, null, null, null, "google.com", null, null, null, null), //
                // google domain + customer's non-existing duns
                Arrays.asList(null, null, null, null, "google.com", null, null, "000000000", null) //
        );
        String expectedEntityId = null;
        for (List<Object> dataItem : dataList) {
            output = matchAccount(dataItem, true, tenant, null, FIELDS, null).getRight();
            String entityId = verifyAndGetEntityId(output);
            Assert.assertNotNull(entityId);
            if (expectedEntityId == null) {
                expectedEntityId = entityId;
            } else {
                Assert.assertEquals(entityId, expectedEntityId);
            }
        }
    }

    /**
     * Use USA as default country if Country is not mapped as MatchKey or
     * Country is not provided with value
     */
    @Test(groups = "functional", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testDefaultCountry() {
        String tenantId = createNewTenantId();
        Tenant tenant = new Tenant(tenantId);

        // Data schema: ID_ACCT, ID_MKTO, ID_ELOQUA, Name, Domain, Country,
        // State, DUNS, preferred ID

        // Domain + Country
        // Country is mapped in match key without value
        List<Object> data = Arrays.asList(null, null, null, null, "google.com", "usa", null, null, null);
        MatchOutput output = matchAccount(data, true, tenant, null, FIELDS, null).getRight();
        String entityId1 = verifyAndGetEntityId(output);
        data = Arrays.asList(null, null, null, null, "google.com", null, null, null, null);
        output = matchAccount(data, true, tenant, null, FIELDS, null).getRight();
        String entityId2 = verifyAndGetEntityId(output);
        Assert.assertEquals(entityId1, entityId2);
        // Country is not mapped in match key
        List<String> keys = Arrays.asList(MatchKey.Domain.name());
        Pair<MatchInput, MatchOutput> inputOutput = matchAccount(data, true, tenant,
                getEntityKeyMap(keys), FIELDS, null);
        String entityId3 = verifyAndGetEntityId(inputOutput.getRight());
        Assert.assertEquals(entityId1, entityId3);

        // Name + Country
        // Country is mapped in match key without value
        data = Arrays.asList(null, null, null, "google", null, "usa", null, null, null);
        output = matchAccount(data, true, tenant, null, FIELDS, null).getRight();
        entityId1 = verifyAndGetEntityId(output);
        data = Arrays.asList(null, null, null, "google", null, null, null, null, null);
        output = matchAccount(data, true, tenant, null, FIELDS, null).getRight();
        entityId2 = verifyAndGetEntityId(output);
        Assert.assertEquals(entityId1, entityId2);
        // Country is not mapped in match key
        keys = Arrays.asList(MatchKey.Name.name());
        inputOutput = matchAccount(data, true, tenant, getEntityKeyMap(keys), FIELDS, null);
        entityId3 = verifyAndGetEntityId(inputOutput.getRight());
        Assert.assertEquals(entityId1, entityId3);
    }

    /**
     * Test scenario: Able to find a matched MatchKey and there is no conflict
     * in MatchKeys with higher priority
     * Expectation: Match to same Account
     *
     * Run 3 test for each test case
     *
     * Test 1: Construct 2 records. 1st record with full Account MatchKeys
     * (ACCOUNT_KEYS_PRIORITIZED), 2nd record with partial Account MatchKeys
     * (try all the combo). So MatchKeys intersection between 2 records is just
     * 2nd record's MatchKeys. Set 2nd record's HIGHEST priority MatchKey with
     * same input data with 1st record and all the other LOWER priority keys
     * with different input data
     *
     * eg. Record1: [acct_id_1, mkto_id_1, 060902413, fakedomain1.com, fakename1]
     * Record2: [mkto_id_1, 884745530, fakedomain2.com]
     *          (ID_MKTO is highest priority key)
     *
     * Test 2: Construct 2 records. MatchKeys preparation is same as Test1,
     * while data preparation is different. Set 2nd record's LOWEST priority
     * MatchKey with same input data with 1st record and all the other HIGHER
     * priority keys with different input data. Meanwhile, set 1st record with
     * null value for those higher priority MatchKeys of 2ND record
     *
     * eg. Record1: [acct_id_1, null, null, fakedomain1.com, fakename1]
     * Record2: [mkto_id_2, 884745530, fakedomain1.com]
     *
     * Test 3: Construct 2 records. MatchKey preparation is same as Test2, and
     * data preparation is very similar except the following case: If HIGHER
     * priority keys in 2NG record are many-to-many relationship to account
     * (Domain/Name), DO NOT set 1ST record with null value for these HIGHER
     * priority MatchKeys
     *
     * eg. Record1: [acct_id_1, null, null, fakedomain1.com, fakename1]
     * Record2: [mkto_id_2, 884745530, fakedomain2.com, fakename1]
     *
     * NOTE for data preparation: Prepare both Domain and Name with values which
     * don't exist in AccountMaster so that DUNS used in Account match is purely
     * from input
     *
     * @param caseIdx
     * @param partialKeys:
     *            Guaranteed to be prioritized
     */
    @Test(groups = "functional", dataProvider = "allKeysComboPrioritized", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testPairsWithHighPriKeyMatch(Integer caseIdx, List<String> partialKeys) {
        List<String> fullKeys = Arrays.asList(ACCOUNT_KEYS_PRIORITIZED);
        log.info("CaseIdx: {} (Out of 31)   Full Keys: {}   Partial Keys: {}", caseIdx, String.join(",", fullKeys),
                String.join(",", partialKeys));
        // Use different tenant for each test case because data provider is set
        // with parallel execution
        String tenantId = createNewTenantId();
        Tenant tenant = new Tenant(tenantId);

        // Data schema: ID_ACCT, ID_MKTO, ID_ELOQUA, Name, Domain, Country,
        // State, DUNS (ID_ELOQUA, Country, State are not used in this test)
        // DUNS needs to be real DUNS otherwise LDC match will not return
        // DUNS to Account match
        List<Object> baseData1 = Arrays.asList("acct_id_1", "mkto_id_1", null, "fakename1", "fakedomain1.com", null,
                null, "060902413", null);
        List<Object> baseData2 = Arrays.asList("acct_id_2", "mkto_id_2", null, "fakename2", "fakedomain2.com", null,
                null, "884745530", null);

        // Test 1
        List<Object> test1Data1 = baseData1;
        List<Object> test1Data2 = new ArrayList<>(Collections.nCopies(baseData2.size(), null));
        // partialKeys is guaranteed to be prioritized already
        // Set 2nd record's HIGHEST priority MatchKey with same input data with
        // 1st record
        test1Data2.set(ACCOUNT_KEYIDX_MAP.get(partialKeys.get(0)),
                baseData1.get(ACCOUNT_KEYIDX_MAP.get(partialKeys.get(0))));
        // Set all the other lower priority keys in 2nd record with different
        // input data compared to 1st record
        IntStream.range(1, partialKeys.size()).forEach(idx -> {
            test1Data2.set(ACCOUNT_KEYIDX_MAP.get(partialKeys.get(idx)),
                    baseData2.get(ACCOUNT_KEYIDX_MAP.get(partialKeys.get(idx))));
        });
        runAndVerifyMatchPair(tenant, fullKeys, test1Data1, partialKeys, test1Data2, true);

        // Test 2
        if (partialKeys.size() == 1) {
            return;
        }
        testEntityMatchService.bumpVersion(tenantId);
        List<Object> test2Data1 = new ArrayList<>(baseData1);
        List<Object> test2Data2 = new ArrayList<>(Collections.nCopies(baseData2.size(), null));
        // Set 2nd record's LOWEST priority MatchKey with same input data with
        // 1st record.
        test2Data2.set(ACCOUNT_KEYIDX_MAP.get(partialKeys.get(partialKeys.size() - 1)),
                baseData1.get(ACCOUNT_KEYIDX_MAP.get(partialKeys.get(partialKeys.size() - 1))));
        // Set all the other higher priority keys in 2nd record with different
        // input data compared to 1st record
        IntStream.range(0, partialKeys.size() - 1).forEach(idx -> {
            test2Data2.set(ACCOUNT_KEYIDX_MAP.get(partialKeys.get(idx)),
                    baseData2.get(ACCOUNT_KEYIDX_MAP.get(partialKeys.get(idx))));
        });
        // Set 1st record with null value for those higher priority MatchKeys of
        // 2ND record
        IntStream.range(0, partialKeys.size() - 1).forEach(idx -> {
            test2Data1.set(ACCOUNT_KEYIDX_MAP.get(partialKeys.get(idx)), null);
        });
        runAndVerifyMatchPair(tenant, fullKeys, test2Data1, partialKeys, test2Data2, true);

        // Test 3 (Only if higher priority keys in 2nd record are many-to-many
        // relationship to account (Domain/Name))
        if (!IntStream.range(0, partialKeys.size() - 1).boxed()
                .anyMatch(idx -> ACCOUNT_KEYS_MANY_TO_MANY.contains(partialKeys.get(idx)))) {
            return;
        }
        testEntityMatchService.bumpVersion(tenantId);
        List<Object> test3Data1 = new ArrayList<>(baseData1);
        List<Object> test3Data2 = new ArrayList<>(Collections.nCopies(baseData2.size(), null));
        // Set 2nd record's LOWEST priority MatchKey with same input data with
        // 1st record.
        test3Data2.set(ACCOUNT_KEYIDX_MAP.get(partialKeys.get(partialKeys.size() - 1)),
                baseData1.get(ACCOUNT_KEYIDX_MAP.get(partialKeys.get(partialKeys.size() - 1))));
        // Set all the other higher priority keys in 2nd record with different
        // input data compared to 1st record
        IntStream.range(0, partialKeys.size() - 1).forEach(idx -> {
            test3Data2.set(ACCOUNT_KEYIDX_MAP.get(partialKeys.get(idx)),
                    baseData2.get(ACCOUNT_KEYIDX_MAP.get(partialKeys.get(idx))));
        });
        // Set 1st record with null value for those higher priority MatchKeys of
        // 2ND record which DO NOT have many-to-many relationship to Account
        IntStream.range(0, partialKeys.size() - 1).forEach(idx -> {
            if (!ACCOUNT_KEYS_MANY_TO_MANY.contains(partialKeys.get(idx))) {
                test3Data1.set(ACCOUNT_KEYIDX_MAP.get(partialKeys.get(idx)), null);
            }
        });
        runAndVerifyMatchPair(tenant, fullKeys, test3Data1, partialKeys, test3Data2, true);
    }

    /**
     * Test scenario: Highest matched priority key has conflict
     * Expectation: Match to different Account
     *
     * Construct 2 records. 1st record with full Account MatchKeys
     * (ACCOUNT_KEYS_PRIORITIZED), 2nd record with partial Account MatchKeys
     * (try all the combo). So MatchKeys intersection between 2 records is just
     * 2nd record's MatchKeys. Set 2nd record's HIGHEST priority MatchKey with
     * different input data with 1st record and all the other LOWER priority keys
     * with same input data. If 2nd record's HIGHEST priority MatchKey has
     * many-to-many relationship with Account, between match of 1st and 2nd records,
     * need to submit another match whose highest priority MatchKey has same value
     * as 2nd record and all other MatchKeys as null to make sure another EntityId
     * is allocated. Thus we can make sure when matching 2nd record, highest
     * priority key has conflict with 1st record
     *
     * eg1. Record1: [acct_id_1, mkto_id_1, 060902413, fakedomain1.com, fakename1]
     * Record2: [mkto_id_2, 060902413, fakedomain1.com] (ID_MKTO is highest priority key)
     * eg2. Record1: [acct_id_1, mkto_id_1, 060902413, fakedomain1.com, fakename1]
     * Record2: [fakedomain2.com, fakename1]
     * (Domain is highest priority key. Need to allocate another EntityId for fakedomain2.com first)
     *
     * NOTE for data preparation: Prepare both Domain and Name with values which
     * don't exist in AccountMaster so that DUNS used in Account match is purely
     * from input
     *
     * @param caseIdx
     * @param partialKeys:
     *            Guaranteed to be prioritized
     */
    @Test(groups = "functional", dataProvider = "allKeysComboPrioritized", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testPairsWithHighPriKeyMismatch(Integer caseIdx, List<String> partialKeys) {
        List<String> fullKeys = Arrays.asList(ACCOUNT_KEYS_PRIORITIZED);
        log.info("CaseIdx: {} (Out of 31)   Full Keys: {}   Partial Keys: {}", caseIdx, String.join(",", fullKeys),
                String.join(",", partialKeys));
        String tenantId = createNewTenantId();
        Tenant tenant = new Tenant(tenantId);

        // Data schema: ID_ACCT, ID_MKTO, ID_ELOQUA, Name, Domain, Country,
        // State, DUNS, Preferred ID (ID_ELOQUA, Country, State are not used in this
        // test)
        // DUNS needs to be real DUNS otherwise LDC match will not return
        // DUNS to Account match
        List<Object> baseData1 = Arrays.asList("acct_id_1", "mkto_id_1", null, "fakename1", "fakedomain1.com", null,
                null, "060902413", null);
        List<Object> baseData2 = Arrays.asList("acct_id_2", "mkto_id_2", null, "fakename2", "fakedomain2.com", null,
                null, "884745530", null);

        List<Object> data1 = baseData1;
        List<Object> data2 = new ArrayList<>(Collections.nCopies(baseData2.size(), null));
        // partialKeys is guaranteed to be prioritized already
        // Set 2nd record's HIGHEST priority MatchKey with different input data
        // with 1st record
        data2.set(ACCOUNT_KEYIDX_MAP.get(partialKeys.get(0)),
                baseData2.get(ACCOUNT_KEYIDX_MAP.get(partialKeys.get(0))));
        // Set all the other lower priority keys in 2nd record with same
        // input data compared to 1st record
        IntStream.range(1, partialKeys.size()).forEach(idx -> {
            data2.set(ACCOUNT_KEYIDX_MAP.get(partialKeys.get(idx)),
                    baseData1.get(ACCOUNT_KEYIDX_MAP.get(partialKeys.get(idx))));
        });
        // If 2nd record's HIGHEST priority MatchKey has many-to-many
        // relationship with Account, between match of 1st and 2nd records, need
        // to submit another match whose highest priority MatchKey has same
        // value as 2nd record. So that we can make sure when matching 2nd
        // record, highest priority key has conflict with 1st record
        if (ACCOUNT_KEYS_MANY_TO_MANY.contains(partialKeys.get(0))) {
            List<Object> data3 = new ArrayList<>(Collections.nCopies(baseData2.size(), null));
            data3.set(ACCOUNT_KEYIDX_MAP.get(partialKeys.get(0)),
                    baseData2.get(ACCOUNT_KEYIDX_MAP.get(partialKeys.get(0))));
            matchAccount(data3, true, tenant, getEntityKeyMap(partialKeys), FIELDS, null);
        }
        runAndVerifyMatchPair(tenant, fullKeys, data1, partialKeys, data2, false);
    }

    /**
     * Test scenario: No any matched keys
     * Expectation: Match to different Account
     *
     * Construct 2 records. Both of them try all combinations of MatchKeys.
     * For non-overlapped keys, set with same input values.
     * For overlapped keys, set with different input values.
     *
     * NOTE for data preparation: Prepare both Domain and Name with values which
     * don't exist in AccountMaster so that DUNS used in Account match is purely
     * from input
     *
     * @param caseIdx
     * @param keys1
     * @param keys2
     */
    @Test(groups = "functional", dataProvider = "allReducedKeysPairs", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testPairAllMismatch(Integer caseIdx, List<String> keys1, List<String> keys2) {
        log.info("CaseIdx: {} (Out of 225)   Keys1: {}   Keys2: {}", caseIdx, String.join(",", keys1),
                String.join(",", keys2));
        String tenantId = createNewTenantId();
        Tenant tenant = new Tenant(tenantId);

        // Data schema: ID_ACCT, ID_MKTO, ID_ELOQUA, Name, Domain, Country,
        // State, DUNS (ID_MKTO, ID_ELOQUA, Country, State are not used in this
        // test)
        // DUNS needs to be real DUNS otherwise LDC match will not return
        // DUNS to Account match
        List<Object> baseData1 = Arrays.asList("acct_id_1", null, null, "fakename1", "fakedomain1.com", null,
                null, "060902413", null);
        List<Object> baseData2 = Arrays.asList("acct_id_2", null, null, "fakename2", "fakedomain2.com", null,
                null, "884745530", null);

        List<String> keysIntersection = getKeysIntersection(keys1, keys2);
        List<Object> data1 = new ArrayList<>(Collections.nCopies(baseData1.size(), null));
        List<Object> data2 = new ArrayList<>(Collections.nCopies(baseData1.size(), null));
        // Use baseData1 to initialize both 2 records based on their MatchKeys
        IntStream.range(0, keys1.size()).forEach(idx -> {
            data1.set(ACCOUNT_KEYIDX_MAP.get(keys1.get(idx)), baseData1.get(ACCOUNT_KEYIDX_MAP.get(keys1.get(idx))));
        });
        IntStream.range(0, keys2.size()).forEach(idx -> {
            data2.set(ACCOUNT_KEYIDX_MAP.get(keys2.get(idx)), baseData1.get(ACCOUNT_KEYIDX_MAP.get(keys2.get(idx))));
        });
        // For overlapped keys, set with different input values
        IntStream.range(0, keysIntersection.size()).forEach(idx -> {
            data2.set(ACCOUNT_KEYIDX_MAP.get(keysIntersection.get(idx)),
                    baseData2.get(ACCOUNT_KEYIDX_MAP.get(keysIntersection.get(idx))));
        });
        runAndVerifyMatchPair(tenant, keys1, data1, keys2, data2, false);
    }

    /**
     * Test scenario: All MatchKeys are matched
     * Expectation: Match to same Account
     *
     * Definition of MatchKey group: see comment of variable KEY_GROUP_MAP
     *
     * Every time construct 2 records.
     * Each record uses a sets of MatchKey groups as match keys.
     * 2 records have some overlapped MatchKey groups and all overlapped MatchKey groups have matched value (no conflict)
     * Submit 2 records to match service sequentially and should always return same EntityId
     *
     * eg.
     * Record 1: MatchKey group = ID_ACCT + DUNS, MatchKey = ID_ACCT + Name + Country
     * Record 2: MatchKey group = ID_MKTO + DUNS, MatchKey = ID_MKTO + Customer DUNS
     * Even if Record1 and Record2 don't have overlap in MatchKey,
     * since we design Name + Country in Record1 to match to Customer DUNS in Record2 by DnB,
     * they do overlapped MatchKeys in Account match
     *
     * NOTE: This test takes 3.5 mins and totally 916 test cases
     */
    @Test(groups = "functional", dataProvider = "exhaustiveKeysPairWithOverlap", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testMatchedPairs(Integer caseIdx, List<String> keys1, List<String> keys2) {
        log.info("CaseIdx: {} (Out of 916)   Keys1: {}   Keys2: {}", caseIdx, String.join(",", keys1),
                String.join(",", keys2));

        // Use different tenant for each test case because data provider is set
        // with parallel execution
        String tenantId = createNewTenantId();
        Tenant tenant = new Tenant(tenantId);
        // Data and Fields are all the same, but MatchKey passed to MatchInput
        // are different
        List<Object> data = Arrays.asList("acct_id", "mkto_id", "eloqua_id", "google", "google.com", "usa", "ca",
                "060902413", null);

        runAndVerifyMatchPair(tenant, keys1, data, keys2, data, true);
    }

    /**
     * Make sure we match to anonymous account when user does not map any account
     * match key
     *
     * @param accountKeyMap
     */
    @Test(groups = "functional", dataProvider = "noUserMappingKeyMap", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testNoUserMapping(EntityKeyMap accountKeyMap) {
        Tenant tenant = newTestTenant();
        List<Object> data = Arrays.asList("acct_id", "mkto_id", "eloqua_id", "google", "google.com", "usa", "ca",
                "060902413", null);
        Pair<MatchInput, MatchOutput> inputOutput = matchAccount(data, true, tenant, accountKeyMap, FIELDS, false,
                null);
        String entityId = verifyAndGetEntityId(inputOutput.getRight());
        Assert.assertEquals(entityId, DataCloudConstants.ENTITY_ANONYMOUS_ID, String
                .format("Should match to anonymous account with EntityKeyMap=%s", JsonUtils.serialize(accountKeyMap)));
    }

    /**
     * Test conflict system IDs are correctly cleared out in the input row of match
     * result
     */
    @Test(groups = "functional", dataProvider = "conflictSystemId", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testClearConflictSystemIds(ClearConflictIDTestCase testCase) {
        Tenant tenant = newTestTenant();

        // prepare existing data
        EntityKeyMap entityKeyMap = getEntityKeyMap();
        for (String[] data : testCase.existingData) {
            matchAccount(Arrays.asList(data), true, tenant, entityKeyMap, FIELDS, null);
        }

        // match with input data
        Pair<MatchInput, MatchOutput> result = matchAccount(Arrays.asList(testCase.inputData), true, tenant,
                entityKeyMap, FIELDS, null);
        Assert.assertNotNull(result.getRight(), "MatchOutput in result should not be null");

        MatchOutput output = result.getRight();
        Assert.assertNotNull(output.getResult(), "Output result list should not be null");
        Assert.assertEquals(output.getResult().size(), 1, "Output result list should only have one entry");

        OutputRecord record = output.getResult().get(0);
        Assert.assertNotNull(record, "Output record at idx 0 should not be null");
        Assert.assertNotNull(record.getInput(), "match input row in output record should not be null");
        Assert.assertEquals(record.getInput().size(), testCase.inputData.length,
                "match input row in output record should have the same length as test case input");

        // check whether conflict system IDs are cleared in input row in the record
        List<Object> resultInput = record.getInput();
        for (int i = 0; i < resultInput.size(); i++) {
            Assert.assertEquals(resultInput.get(i), testCase.expectedInputAfterMatch[i],
                    String.format("Expected input does not match result input at idx=%d", i));
        }
    }

    /**
     * System ID matching should be case in-sensitive
     */
    @Test(groups = "functional", dataProvider = "caseInsensitiveSystemIdMatch", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testCaseInsensitiveSystemIdMatch(String customerAccountId) {
        Tenant tenant = newTestTenant();

        // populate original account ID
        String expectedEntityId = matchCustomerAccountId(customerAccountId, tenant);
        Assert.assertNotNull(expectedEntityId);

        // match again with the same ID
        String entityId = matchCustomerAccountId(customerAccountId, tenant);
        Assert.assertEquals(entityId, expectedEntityId, String
                .format("Match again with CustomerAccountId=%s should match to the same account", customerAccountId));

        // match with lower case ID
        String lowerCaseEntityId = matchCustomerAccountId(customerAccountId.toLowerCase(), tenant);
        Assert.assertEquals(lowerCaseEntityId, expectedEntityId, String.format(
                "Matching with lower case ID should match to the same account as original one. CustomerAccountId=%s",
                customerAccountId));

        // match with upper case ID
        String upperCaseEntityId = matchCustomerAccountId(customerAccountId.toUpperCase(), tenant);
        Assert.assertEquals(upperCaseEntityId, expectedEntityId, String.format(
                "Matching with upper case ID should match to the same account as original one. CustomerAccountId=%s",
                customerAccountId));
    }

    private String matchCustomerAccountId(@NotNull String customerAccountId, @NotNull Tenant tenant) {
        List<Object> data = Arrays.asList(customerAccountId, null, null, null, null, null, null, null, null);
        Pair<MatchInput, MatchOutput> result = matchAccount(data, true, tenant, getEntityKeyMap(), FIELDS, null);
        String entityId = verifyAndGetEntityId(result.getRight());
        // after verifyAndGetEntityId, all intermediate object should be non-null
        String outputCustomerAccountId = (String) result.getRight().getResult().get(0).getInput().get(0);
        Assert.assertEquals(outputCustomerAccountId, customerAccountId,
                "CustomerAccountId in output record's input should be the same as the original one");
        return entityId;
    }

    private void runAndVerifyMatchPair(Tenant tenant, List<String> keys1, List<Object> data1, List<String> keys2,
            List<Object> data2, boolean isMatched) {
        Pair<MatchInput, MatchOutput> inputOutput1 = matchAccount(data1, true, tenant,
                getEntityKeyMap(keys1), FIELDS, null);
        MatchOutput output1 = inputOutput1.getRight();
        String entityId1 = verifyAndGetEntityId(output1);
        Assert.assertNotNull(entityId1, String.format("EntityId got null for Keys=%s   MatchInput=%s",
                String.join(",", keys1), JsonUtils.serialize(inputOutput1.getLeft())));

        Pair<MatchInput, MatchOutput> inputOutput2 = matchAccount(data2, true, tenant,
                getEntityKeyMap(keys2), FIELDS, null);
        MatchOutput output2 = inputOutput2.getRight();
        String entityId2 = verifyAndGetEntityId(output2);
        Assert.assertNotNull(entityId2, String.format("EntityId got null for Keys=%s   MatchInput=%s",
                String.join(",", keys2), JsonUtils.serialize(inputOutput2.getLeft())));

        if (isMatched) {
            Assert.assertEquals(entityId1, entityId2, String.format(
                    "EntityIds are expected to same but got different result for Keys1=%s vs Keys2=%s    MatchInput1=%s    MatchInput2=%s",
                    String.join(",", keys1), String.join(",", keys2), JsonUtils.serialize(inputOutput1.getLeft()),
                    JsonUtils.serialize(inputOutput2.getLeft())));
        } else {
            Assert.assertNotEquals(entityId1, entityId2, String.format(
                    "EntityIds are expected to different but got same result for Keys1=%s vs Keys2=%s    MatchInput1=%s    MatchInput2=%s",
                    String.join(",", keys1), String.join(",", keys2), JsonUtils.serialize(inputOutput1.getLeft()),
                    JsonUtils.serialize(inputOutput2.getLeft())));
        }

    }

    /*
     * lookup mode will filter out MatchKey.PreferredEntityId since it's not
     * allowed, other fields are the same as allocateId mode
     */
    private String lookupAccount(Tenant tenant, String acctId, String mktoId, String eloquaId, String name,
            String domain, String country, String state, String duns) {
        List<Object> data = Arrays.asList(acctId, mktoId, eloquaId, name, domain, country, state, duns, null);
        MatchOutput output = matchAccount(data, false, tenant, getEntityKeyMap(false), FIELDS, null).getRight();
        return verifyAndGetEntityId(output);
    }

    private String lookupAccount(Tenant tenant, Integer servingVersion, boolean accountExists, String acctId,
            String mktoId, String eloquaId, String name, String domain, String country, String state, String duns) {
        List<Object> data = Arrays.asList(acctId, mktoId, eloquaId, name, domain, country, state, duns, null);
        MatchOutput output = matchAccount(data, false, tenant, getEntityKeyMap(false), FIELDS, servingVersion)
                .getRight();
        return verifyAndGetEntityId(output, InterfaceName.AccountId.name(), accountExists);
    }

    /**
     * @param data
     * @param isAllocateMode
     * @param tenant
     * @param entityKeyMap:
     *            If null, use getEntityKeyMap(Account)
     * @param fields
     * @param publicDomainAsNormalDomain
     * @return
     */
    private Pair<MatchInput, MatchOutput> matchAccount(List<Object> data, boolean isAllocateMode,
            Tenant tenant, EntityKeyMap entityKeyMap, List<String> fields, boolean publicDomainAsNormalDomain,
            Integer servingVersion) {
        String entity = BusinessEntity.Account.name();
        Map<String, EntityKeyMap> maps = new HashMap<>();
        maps.put(entity, entityKeyMap);
        MatchInput input = prepareEntityMatchInput(tenant, entity, maps, servingVersion);
        input.setAllocateId(isAllocateMode); // Not take effect in this test
        entityMatchConfigurationService.setIsAllocateMode(isAllocateMode);
        input.setFields(fields);
        input.setData(Collections.singletonList(data));
        input.setPublicDomainAsNormalDomain(publicDomainAsNormalDomain);
        return Pair.of(input, realTimeMatchService.match(input));
    }

    /**
     * matchAccount() version which hard codes treating public domains specially.
     *
     * @param data
     * @param isAllocateMode
     * @param tenant
     * @param entityKeyMap:
     *            If null, use getEntityKeyMap(Account)
     * @param fields
     * @return
     */
    private Pair<MatchInput, MatchOutput> matchAccount(List<Object> data, boolean isAllocateMode,
            Tenant tenant, EntityKeyMap entityKeyMap, List<String> fields, Integer servingVersion) {
        if (entityKeyMap == null) {
            entityKeyMap = getEntityKeyMap();
        }
        return matchAccount(data, isAllocateMode, tenant, entityKeyMap, fields, false, servingVersion);
    }

    private static EntityKeyMap getEntityKeyMap() {
        return getEntityKeyMap(true);
    }

    private static EntityKeyMap getEntityKeyMap(boolean allocateId) {
        EntityKeyMap map = new EntityKeyMap();
        Map<MatchKey, List<String>> fieldMap = Arrays.stream(MATCH_KEY_FIELDS)
                .filter(key -> allocateId || MatchKey.PreferredEntityId != key) // filter preferred ID for lookup mode
                .collect(
                        Collectors.toMap(matchKey -> matchKey, matchKey -> Collections.singletonList(matchKey.name())));
        fieldMap.put(MatchKey.SystemId, Arrays.asList(SYSTEM_ID_FIELDS));
        map.setKeyMap(fieldMap);
        return map;
    }

    private static EntityKeyMap getEntityKeyMap(List<String> keys) {
        EntityKeyMap map = new EntityKeyMap();
        Map<MatchKey, List<String>> keyMap = MatchKeyUtils.resolveKeyMap(keys);
        map.setKeyMap(keyMap);
        keys.forEach(key -> {
            switch (key) {
            case ID_ACCT:
            case ID_MKTO:
            case ID_ELOQUA:
                // Priority is same as their order in keys list
                map.addMatchKey(MatchKey.SystemId, key);
                break;
            default:
                break;
            }
        });
        return map;
    }

    /**
     * Construct pairs of MatchKeys accordingly with "overlap" within each pair
     *
     * NOTE: "Overlap" refers MatchKeys used in Account match decision graph, not
     * actual MatchKeys passed into MatchInput
     * See example in comment of method testMatchedPairs()
     *
     * Step 1: Find all possible subsets of keyGroups and try each of them as
     *         overlapped key group (MatchKeys used in Account match decision graph)
     * Step 2: For each overlapKeyGroups (each subset), split remaining MatchKey
     *         groups into 2 (try all possible splits), append to overlapKeyGroups
     *         respectively to construct MatchKey groups for two records.
     * Step 3: Translate MatchKey groups to MatchKeys.
     * Step 4: Do all necessary dedup in MatchKeys pair because MatchKeys
     *         mapped to different groups are not disjoint
     *
     * @return {{CaseIdx, List<String>, List<String>},
     *          {CaseIdx, List<String>, List<String>},
     *          ...}
     */
    @DataProvider(name = "exhaustiveKeysPairWithOverlap", parallel = true)
    private static Object[][] getExhaustiveKeysPairWithOverlap() {
        List<String> keyGroups = new ArrayList<>(KEY_GROUP_MAP.keySet());
        // Step 1
        List<List<String>> allKeyGroupsSubsets = getAllKeyGroupSubsets(keyGroups);
        List<Pair<List<String>, List<String>>> keyGroupsPairs = new ArrayList<>();
        for (List<String> overlapKeyGroups : allKeyGroupsSubsets) {
            List<String> remainKeyGroups = getComplementaryKeyGroups(keyGroups, overlapKeyGroups);
            List<Pair<List<String>, List<String>>> remainKeyGroupsPairs = getAllDisjointSubsetPairs(remainKeyGroups);
            // Step 2
            for (Pair<List<String>, List<String>> pair : remainKeyGroupsPairs) {
                List<String> keyGroups1 = new ArrayList<>(overlapKeyGroups);
                keyGroups1.addAll(pair.getLeft());
                List<String> keyGroups2 = new ArrayList<>(overlapKeyGroups);
                keyGroups2.addAll(getComplementaryKeyGroups(remainKeyGroups, pair.getRight()));
                keyGroupsPairs.add(Pair.of(keyGroups1, keyGroups2));
            }
        }
        // Step 3
        List<Pair<List<String>, List<String>>> keysPairs = new ArrayList<>();
        for (Pair<List<String>, List<String>> keyGroupsPair : keyGroupsPairs) {
            List<List<String>> keysCombo1 = keyGroupsToKeys(keyGroupsPair.getLeft());
            List<List<String>> keysCombo2 = keyGroupsToKeys(keyGroupsPair.getRight());
            for (List<String> keys1 : keysCombo1)
                for (List<String> keys2 : keysCombo2) {
                    keysPairs.add(Pair.of(keys1, keys2));
                }
        }
        // Step 4
        List<Pair<List<String>, List<String>>> dedupKeysPairs = dedupMatchkeyPairs(keysPairs);
        return IntStream.range(0, dedupKeysPairs.size()).mapToObj(
                idx -> new Object[] { idx, dedupKeysPairs.get(idx).getLeft(), dedupKeysPairs.get(idx).getRight() })
                .toArray(Object[][]::new);
    }

    /**
     * @return {{CaseIdx, List<String>, List<String>},
     *          {CaseIdx, List<String>, List<String>},
     *          ...}
     */
    @DataProvider(name = "allKeysComboPrioritized", parallel = true)
    private static Object[][] getAllKeysComboPrioritized() {
        List<String> accountKeys = Arrays.asList(ACCOUNT_KEYS_PRIORITIZED);
        List<List<String>> allCombos = getAllKeyGroupSubsets(accountKeys);
        return IntStream.range(0, allCombos.size()).mapToObj(idx -> new Object[] { idx, allCombos.get(idx) })
                .toArray(Object[][]::new);
    }

    /**
     * @return {{CaseIdx, List<String>, List<String>},
     *          {CaseIdx, List<String>, List<String>},
     *          ...}
     */
    @DataProvider(name = "allReducedKeysPairs", parallel = true)
    private static Object[][] getAllReducedKeysPairs() {
        List<String> accountKeys = Arrays.asList(ACCOUNT_KEYS_REDUCED);
        List<List<String>> allCombos = getAllKeyGroupSubsets(accountKeys);
        List<Pair<List<String>, List<String>>> keysPairs = new ArrayList<>();
        for (List<String> keys1 : allCombos)
            for (List<String> keys2 : allCombos) {
                keysPairs.add(Pair.of(keys1, keys2));
            }
        return IntStream.range(0, keysPairs.size())
                .mapToObj(idx -> new Object[] { idx, keysPairs.get(idx).getLeft(), keysPairs.get(idx).getRight() })
                .toArray(Object[][]::new);
    }

    /**
     * Find all possible subsets of key groups
     *
     * Returned keys in each subset are guaranteed to be in same order as
     * original input list
     *
     * @param keyGroups
     * @return
     */
    private static List<List<String>> getAllKeyGroupSubsets(List<String> keyGroups) {
        int total = (int) Math.pow(2d, Double.valueOf(keyGroups.size()));
        List<List<String>> combos = new ArrayList<>();
        // To verify correctness of all combinations
        Set<String> comboSet = new HashSet<>();
        for (int i = 1; i < total; i++) {
            String code = Integer.toBinaryString(total | i).substring(1);
            List<String> combo = new ArrayList<>();
            for (int j = 0; j < keyGroups.size(); j++) {
                if (code.charAt(j) == '1') {
                    combo.add(keyGroups.get(j));
                }
            }
            combos.add(combo);
            comboSet.add(String.join("", combo));
        }
        // Verify there is no duplicate in all combinations
        Assert.assertEquals(comboSet.size(), total - 1);
        // Verify keys order in each subset is maintained as original input list
        // key/key group -> idx of key/key group in input list keyGroups
        Map<String, Integer> idxMap = IntStream.range(0, keyGroups.size()).boxed()
                .collect(Collectors.toMap(idx -> keyGroups.get(idx), idx -> idx));
        combos.forEach(combo -> {
            Assert.assertFalse(IntStream.range(1, combo.size())
                    .anyMatch(i -> idxMap.get(combo.get(i)) <= idxMap.get(combo.get(i - 1))));
        });
        return combos;
    }

    /**
     * Return a list of key groups from completeKeyGroups which do not exist in
     * keyGroups
     *
     * @param completeKeyGroups
     * @param keyGroups
     * @return
     */
    private static List<String> getComplementaryKeyGroups(List<String> completeKeyGroups, List<String> keyGroups) {
        Set<String> keyGroupsSet = new HashSet<>(keyGroups);
        List<String> complementaryKeyGroups = completeKeyGroups.stream()
                .filter(keyGroup -> !keyGroupsSet.contains(keyGroup)).collect(Collectors.toList());
        // Verify correctness of complementary set calculation
        Assert.assertEquals(complementaryKeyGroups.size() + keyGroups.size(), completeKeyGroups.size());
        return complementaryKeyGroups;
    }

    /**
     * Return all pairs of disjoint subsets
     * eg. input = {"a", "b", "c"}
     * return = <{}, {"a", "b", "c"}>
     *          <{"a"}, {}>, <{"a"}, {"b"}>, <{"a"}, {"c"}>, <{"a"}, {"b", "c"}>
     *          <{"b"}, {}>, <{"b"}, {"a"}>, <{"b"}, {"c"}>, <{"b"}, {"a", "c"}>
     *          <{"c"}, {}>, <{"c"}, {"b"}>, <{"c"}, {"a"}>, <{"c"}, {"a", "b"}>
     *          <{"a", "b"}, {"c"}>, <{"a", "c"}, {"b"}>
     *          <{"b", "c"}, {"a"}>, <{"a", "b", "c"}, {}>
     *
     * Intentionally return Pair<groups1, groups2> and Pair<groups2, groups1> separately,
     * because it matters to which MatchKey group is submitted to match first
     *
     * @param keyGroups
     * @return
     */
    private static List<Pair<List<String>, List<String>>> getAllDisjointSubsetPairs(List<String> keyGroups) {
        List<List<String>> allSubSets = getAllKeyGroupSubsets(keyGroups);
        allSubSets.add(new ArrayList<>());
        List<Pair<List<String>, List<String>>> allDisjointSubsetPairs = new ArrayList<>();
        for (List<String> groups1 : allSubSets) {
            for (List<String> groups2 : allSubSets) {
                if (Collections.disjoint(groups1, groups2)) {
                    allDisjointSubsetPairs.add(Pair.of(groups1, groups2));

                }
            }
        }
        return allDisjointSubsetPairs;
    }

    /**
     * With a list of MatchKey groups, return all combinations of MatchKeys
     * (deduped) accordingly
     *
     * eg. keyGroups = [ID_ACCT, DUNS, Name]
     * After DFS keys = [ID_ACCT, Customer DUNS, Name],
     *                  [ID_ACCT, Customer DUNS, Name, Country],
     *                  [ID_ACCT, Domain, Country, Name],
     *                  [ID_ACCT, Domain, Country, Name], (Deduped from [ID_ACCT, Domain, Country, Name, Country])
     *                  [ID_ACCT, Name, Country], (Deduped from [ID_ACCT, Name, Country, Name])
     *                  [ID_ACCT, Name, Country], (Deduped from [ID_ACCT, Name, Country, Name, Country])
     * After dedup keys = [ID_ACCT, Customer DUNS, Name],
     *                    [ID_ACCT, Customer DUNS, Name, Country],
     *                    [ID_ACCT, Domain, Country, Name],
     *                    [ID_ACCT, Name, Country]
     *
     * @param keyGroups
     * @return
     */
    private static List<List<String>> keyGroupsToKeys(List<String> keyGroups) {
        List<List<String>> toReturn = new ArrayList<>();
        if (CollectionUtils.isEmpty(keyGroups)) {
            return toReturn;
        }
        // Pair<idx in keyGroups list, idx in KEY_GROUP_MAP.get(keyGroup)>
        Stack<Pair<Integer, Integer>> idxSt = new Stack<>();
        Set<String> visited = new HashSet<>();
        idxSt.push(Pair.of(0, 0));
        // Use DFS to find all combinations of MatchKeys (before dedup)
        while (!idxSt.isEmpty()) {
            Pair<Integer, Integer> idxPair = idxSt.peek();
            if (idxPair.getLeft() < keyGroups.size() - 1
                    && !visited.contains(concatIdxes(idxPair.getLeft(), idxPair.getRight()))) {
                visited.add(concatIdxes(idxPair.getLeft(), idxPair.getRight()));
                idxSt.push(Pair.of(idxPair.getLeft() + 1, 0));
                continue;
            }
            visited.add(concatIdxes(idxPair.getLeft(), idxPair.getRight()));
            if (idxPair.getLeft() == keyGroups.size() - 1) {
                Set<String> keys = new HashSet<>();
                for (Pair<Integer, Integer> pair : idxSt) {
                    // Use set to dedup within one combination
                    // eg. [ID_ACCT, Name, Country, Name, Country] -> [ID_ACCT,
                    // Name, Country]
                    keys.addAll(KEY_GROUP_MAP.get(keyGroups.get(pair.getLeft())).get(pair.getRight()));
                }
                toReturn.add(new ArrayList<>(keys));
            }
            if (idxPair.getRight() < KEY_GROUP_MAP.get(keyGroups.get(idxPair.getLeft())).size() - 1) {
                visited.remove(concatIdxes(idxPair.getLeft(), idxPair.getRight()));
                idxSt.pop();
                idxSt.push(Pair.of(idxPair.getLeft(), idxPair.getRight() + 1));
                continue;
            }
            if (idxPair.getRight() == KEY_GROUP_MAP.get(keyGroups.get(idxPair.getLeft())).size() - 1) {
                visited.remove(concatIdxes(idxPair.getLeft(), idxPair.getRight()));
                idxSt.pop();
            }
        }
        int total = 1;
        for (String keyGroup : keyGroups) {
            total *= KEY_GROUP_MAP.get(keyGroup).size();
        }
        // Verify all combinations of match keys are covered
        Assert.assertEquals(toReturn.size(), total);
        return dedupMatchKeys(toReturn);
    }

    @DataProvider(name = "noUserMappingKeyMap")
    private Object[][] noUserMappingKeyMapTestData() {
        return new Object[][] { //
                { null }, // null account key map
                { new EntityKeyMap() }, // key map with null map for match key -> column names
                { new EntityKeyMap(new HashMap<>()) }, // key map with empty map for match key -> column names
        }; //
    }

    @DataProvider(name = "conflictSystemId")
    private Object[][] conflictSystemIdTestData() {
        return new Object[][] { //
                { //
                        /*
                         * match on account ID, mkto id already have diff value in existing account
                         */
                        new ClearConflictIDTestCase( //
                                new String[][] { //
                                        { "acct_id_1", "mkto_id_1", null, "fakename1", "fakedomain1.com", null, null,
                                                "060902413", null } //
                                }, //
                                new String[] { "acct_id_1", "mkto_id_2", null, "fakename1", "fakedomain1.com", null,
                                        null, "060902413", null },
                                new String[] { "acct_id_1", null, null, "fakename1", "fakedomain1.com", null, null,
                                        "060902413", null }) //
                }, //
                { //
                        /*
                         * match on account ID, mkto id in input already taken by another account
                         */
                        new ClearConflictIDTestCase( //
                                new String[][] { //
                                        { "acct_id_1", null, null, null, null, null, null, null, null }, //
                                        { "acct_id_2", "mkto_id_2", null, null, null, null, null, null, null } //
                                }, //
                                new String[] { "acct_id_1", "mkto_id_2", "eloqua_id_1", null, "fakedomain1.com", null,
                                        null, "060902413", null },
                                new String[] { "acct_id_1", null, "eloqua_id_1", null, "fakedomain1.com", null, null,
                                        "060902413", null }) //
                }, //
                { //
                        /*
                         * match on mkto id, but matched account have a different account ID (higher
                         * priority), create new account. mkto id in input is already taken, thus
                         * cleared out
                         */
                        new ClearConflictIDTestCase( //
                                new String[][] { //
                                        { "acct_id_2", "mkto_id_2", null, null, null, null, null, null, null } //
                                }, //
                                new String[] { "acct_id_1", "mkto_id_2", "eloqua_id_1", null, "fakedomain1.com", null,
                                        null, "060902413", null },
                                new String[] { "acct_id_1", null, "eloqua_id_1", null, "fakedomain1.com", null, null,
                                        "060902413", null }) //
                }, //
                { //
                        /*
                         * match on account ID, mkto id in input is the same as matched account, no
                         * conflict. domain & DUNS is taken by another account but won't be cleared out.
                         */
                        new ClearConflictIDTestCase( //
                                new String[][] { //
                                        { "acct_id_1", null, null, null, "fakedomain1.com", null, null, "060902413",
                                                null }, //
                                        { "acct_id_2", "mkto_id_2", null, null, null, null, null, null, null } //
                                }, //
                                new String[] { "acct_id_2", "mkto_id_2", "eloqua_id_1", null, "fakedomain1.com", null,
                                        null, "060902413", null },
                                new String[] { "acct_id_2", "mkto_id_2", "eloqua_id_1", null, "fakedomain1.com", null,
                                        null, "060902413", null }) //
                }, //
                { //
                        /*
                         * match on mkto id, existing account have no account ID, no conflict (one
                         * account can have multiple domain, and it won't be cleared anyways).
                         */
                        new ClearConflictIDTestCase( //
                                new String[][] { //
                                        { "acct_id_1", null, null, null, null, null, null, null, null }, //
                                        { null, "mkto_id_2", null, null, "fakedomain2.com", null, null, null, null } //
                                }, //
                                new String[] { "acct_id_2", "mkto_id_2", null, null, "fakedomain1.com", null, null,
                                        "060902413", null },
                                new String[] { "acct_id_2", "mkto_id_2", null, null, "fakedomain1.com", null, null,
                                        "060902413", null }) //
                }, //
                { //
                        /*
                         * match on account id, existing account have no mkto id, no conflict
                         */
                        new ClearConflictIDTestCase( //
                                new String[][] { //
                                        { "acct_id_1", null, null, null, null, null, null, null, null }, //
                                        { null, "mkto_id_2", null, null, "fakedomain1.com", null, null, null, null } //
                                }, //
                                new String[] { "acct_id_1", "mkto_id_1", null, null, "fakedomain1.com", null, null,
                                        "060902413", null },
                                new String[] { "acct_id_1", "mkto_id_1", null, null, "fakedomain1.com", null, null,
                                        "060902413", null }) //
                }, //
        };
    }

    @DataProvider(name = "caseInsensitiveSystemIdMatch", parallel = true)
    private Object[][] caseInsensitiveSystemIdMatchTestData() {
        return new Object[][] { //
                { " abCD12345FdsdkljHFFdjfkFd   " }, // alphabetic
                { "abcdefg" }, // all lower case
                { " 12345 " }, // all number
                { "ZZZZZ" }, // all upper case
                { "aaBbABC__12345xyZ" }, // alphanumeric
        }; //
    }

    private static String concatIdxes(Integer idx1, Integer idx2) {
        return idx1.toString() + "_" + idx2.toString();
    }

    /**
     * Dedup among sublists of MatchKeys in the input list
     *
     * @param keys
     * @return
     */
    private static List<List<String>> dedupMatchKeys(List<List<String>> keys) {
        List<List<String>> toReturn = new ArrayList<>();
        Set<String> keysSet = new HashSet<>();
        for (List<String> k : keys) {
            Collections.sort(k);
            String concact = String.join("_", k);
            if (!keysSet.contains(concact)) {
                toReturn.add(k);
                keysSet.add(concact);
            }
        }
        return toReturn;
    }

    /**
     * Dedup among MatchKeys pairs because MatchKeys mapped to different groups
     * are not disjoint
     *
     * @param keysPairs
     * @return
     */
    private static List<Pair<List<String>, List<String>>> dedupMatchkeyPairs(
            List<Pair<List<String>, List<String>>> keysPairs) {
        List<Pair<List<String>, List<String>>> toReturn = new ArrayList<>();
        Set<String> keysPairsSet = new HashSet<>();
        keysPairs.forEach(keysPair -> {
            Collections.sort(keysPair.getLeft());
            Collections.sort(keysPair.getRight());
            String concact = String.join("_", keysPair.getLeft()) + "_" + String.join("_", keysPair.getRight());
            if (!keysPairsSet.contains(concact)) {
                toReturn.add(keysPair);
                keysPairsSet.add(concact);
            }
        });
        return toReturn;
    }

    private String verifyAndGetEntityId(@NotNull MatchOutput output) {
        return verifyAndGetEntityId(output, InterfaceName.AccountId.name());
    }

    private static List<String> getKeysIntersection(List<String> keys1, List<String> keys2) {
        return keys1.stream().filter(keys2::contains).collect(Collectors.toList());
    }

    @Override
    protected List<String> getExpectedOutputColumns() {
        return Arrays.asList(InterfaceName.EntityId.name(), InterfaceName.AccountId.name(),
                InterfaceName.LatticeAccountId.name());
    }

    @Override
    protected Logger getLogger() {
        return log;
    }

    /*
     * Test class for testClearConflictSystemIds
     */
    private class ClearConflictIDTestCase {
        String[][] existingData;
        String[] inputData;
        String[] expectedInputAfterMatch;

        ClearConflictIDTestCase(String[][] existingData, String[] inputData, String[] expectedInputAfterMatch) {
            this.existingData = existingData;
            this.inputData = inputData;
            this.expectedInputAfterMatch = expectedInputAfterMatch;
        }

        @Override
        public String toString() {
            return "ClearConflictIDTestCase{" + "existingData=" + Arrays.toString(existingData) + ", inputData="
                    + Arrays.toString(inputData) + ", expectedInputAfterMatch="
                    + Arrays.toString(expectedInputAfterMatch) + '}';
        }
    }
}
