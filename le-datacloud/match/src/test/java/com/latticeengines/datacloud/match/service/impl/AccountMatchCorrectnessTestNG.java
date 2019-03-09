package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.exposed.service.RealTimeMatchService;
import com.latticeengines.datacloud.match.service.EntityMatchConfigurationService;
import com.latticeengines.datacloud.match.service.EntityMatchInternalService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.datacloud.match.testframework.TestEntityMatchService;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;


public class AccountMatchCorrectnessTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(AccountMatchCorrectnessTestNG.class);

    private static final String TEST_TENANT_ID = AccountMatchCorrectnessTestNG.class.getSimpleName() + "_"
            + UUID.randomUUID().toString();
    private static final Tenant TEST_TENANT = new Tenant(TEST_TENANT_ID);

    /*************************************************
     * Please DO NOT re-format comments in this class
     *************************************************/

    /**
     * <pre>
     * FIXME change the field headers to use test helper classes Fields:
     * [ SFDC, MKTO, ELOQUA, Name, Domain, Country, State ]
     * </pre>
     */
    private static final String ID_SFDC = "SFDC";
    private static final String ID_MKTO = "MKTO";
    private static final String ID_ELOQUA = "ELOQUA";

    private static final String[] SYSTEM_ID_FIELDS = new String[] { ID_SFDC, ID_MKTO, ID_ELOQUA };
    private static final MatchKey[] MATCH_KEY_FIELDS = new MatchKey[] { MatchKey.Name, MatchKey.Domain,
            MatchKey.Country, MatchKey.State, MatchKey.DUNS };
    private static final String[] NON_SYSTEM_ID_FIELDS = Arrays.stream(MATCH_KEY_FIELDS).map(MatchKey::name)
            .toArray(String[]::new);
    private static final String[] FIELDS = ArrayUtils.addAll(SYSTEM_ID_FIELDS, NON_SYSTEM_ID_FIELDS);

    /**
     * Define 5 Account MatchKey groups: 
     * ID_SFDC, ID_MKTO, DUNS, Domain, Name
     *
     * MatchKey Group -> Actual MatchKey mapping:
     * ID_SFDC -> [ID_SFDC]
     * ID_MKTO -> [ID_MKTO]
     * DUNS -> [Customer DUNS] or [Name + Country] or [Domain +Country]
     * NOTE: Choose above 3 options for DUNS group due to DUNS in Account match is from LDC match.
     *       These 3 are different paths in LDC match to get DUNS
     *       Customer DUNS: DUNS is from customer, then confirm existence in AMLookup
     *       Name + Country: DUNS is from DnB, then confirm existence in AMLookup
     *       Domain + Country: DUNS is from AMLookup
     *       Each path only select 1 set of MatchKeys because this test is focused on Account match, not LDC match
     * Domain -> [Domain + Country]
     * Name -> [Name + Country]
     **/
    private final static Map<String, List<List<String>>> KEY_GROUP_MAP = new HashMap<>();
    static {
        KEY_GROUP_MAP.put(ID_SFDC, Arrays.asList(Arrays.asList(ID_SFDC)));
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
    private final static String[] ACCOUNT_KEYS_PRIORITIZED = { ID_SFDC, ID_MKTO, MatchKey.DUNS.name(),
            MatchKey.Domain.name(), MatchKey.Name.name() };
    // Account MatchKey -> Index in FIELDS
    private final static Map<String, Integer> ACCOUNT_KEYIDX_MAP = Stream.of(ACCOUNT_KEYS_PRIORITIZED)
            .collect(Collectors.toMap(key -> key, key -> Arrays.asList(FIELDS).indexOf(key)));
    // Account MatchKey with many-to-many relationship to Account
    private final static Set<String> ACCOUNT_KEYS_MANY_TO_MANY = new HashSet<>(
            Arrays.asList(MatchKey.Domain.name(), MatchKey.Name.name()));

    private static final EntityMatchEnvironment DEST_ENV = EntityMatchEnvironment.SERVING;

    @Inject
    private RealTimeMatchService realTimeMatchService;

    @Inject
    private TestEntityMatchService testEntityMatchService;

    @Inject
    private EntityMatchInternalService entityMatchInternalService;

    @Inject
    private EntityMatchConfigurationService entityMatchConfigurationService;

    @Test(groups = "functional", priority = 1)
    private void testAllocateAndLookup() {
        // prevent old data from affecting the test
        testEntityMatchService.bumpVersion(TEST_TENANT_ID);

        // Data schema: ID_SFDC, ID_MKTO, ID_ELOQUA, Name, Domain, Country,
        // State, DUNS
        // test allocate mode
        List<Object> data = Arrays.asList("sfdc_1", "mkto_1", null, "GOOGLE", null, "USA", null, null);
        MatchOutput output = matchAccount(data, true, null, null).getRight();
        String entityId = verifyAndGetEntityId(output);

        // publish for testing lookup
        entityMatchInternalService.publishEntity(BusinessEntity.Account.name(), TEST_TENANT, TEST_TENANT, DEST_ENV,
                true);


        // test lookup, make sure we get the correct entity id with each match key
        Assert.assertEquals(lookupAccount("sfdc_1", null, null, null, null, null, null, null), entityId);
        Assert.assertEquals(lookupAccount(null, "mkto_1", null, null, null, null, null, null), entityId);
        Assert.assertEquals(lookupAccount(null, null, null, "GOOGLE", null, "USA", null, null), entityId);
    }

    @Test(groups = "functional", priority = 2)
    private void testPublicDomain() {
        // prevent old data from affecting the test
        testEntityMatchService.bumpVersion(TEST_TENANT_ID);

        // Data schema: ID_SFDC, ID_MKTO, ID_ELOQUA, Name, Domain, Country,
        // State, DUNS
        // public domain without duns/name, and not in email format, treat as
        // normal domain
        List<Object> data = Arrays.asList(null, null, null, null, "gmail.com", "USA", null, null);
        MatchOutput output = matchAccount(data, true, null, null).getRight();
        String publicDomainEntityId = verifyAndGetEntityId(output);
        Assert.assertNotNull(publicDomainEntityId);

        // public domain without duns/name, but in email format, treat as public
        // domain
        data = Arrays.asList(null, null, null, null, "aaa@gmail.com", "USA", null, null);
        output = matchAccount(data, true, null, null).getRight();
        Assert.assertEquals(verifyAndGetEntityId(output), DataCloudConstants.ENTITY_ANONYMOUS_ID);

        // public domain with duns/name, , treat as public domain
        data = Arrays.asList(null, null, null, "public domain company name", "gmail.com", "USA", null, null);
        output = matchAccount(data, true, null, null).getRight();
        String entityId = verifyAndGetEntityId(output);
        Assert.assertNotNull(entityId);
        Assert.assertNotEquals(entityId, publicDomainEntityId);
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
     * (try all the combo). So overlapped MatchKeys between 2 records is just
     * 2nd record's MatchKeys. Set 2nd record's HIGHEST priority MatchKey with
     * same input data with 1st record and all the other LOWER priority keys
     * with different input data
     *
     * eg. Record1: [sfdc_id_1, mkto_id_1, 060902413, fakedomain_1.com, fakename_1]
     * Record2: [mkto_id_1, 884745530, fakedomain_2.com] (ID_MKTO is highest priority key)
     *
     * Test 2: Construct 2 records. MatchKeys preparation is same as Test1,
     * while data preparation is different. Set 2nd record's LOWEST priority
     * MatchKey with same input data with 1st record and all the other HIGHER
     * priority keys with different input data. Also, set 1st record with null
     * value for those higher priority MatchKeys of 2ND record
     *
     * eg. Record1: [sfdc_id_1, null, null, fakedomain_1.com, fakename_1]
     * Record2: [mkto_id_2, 884745530, fakedomain_1.com]
     *
     * Test 3: Construct 2 records. MatchKey preparation is same as Test2, and
     * data preparation is very similar except the following case: If HIGHER
     * priority keys in 2NG record are many-to-many relationship to account
     * (Domain/Name), DO NOT set 1ST record with null value for these HIGHER
     * priority MatchKeys
     *
     * eg. Record1: [sfdc_id_1, null, null, fakedomain_1.com, fakename_1]
     * Record2: [mkto_id_2, 884745530, fakedomain_2.com, fakename_1]
     *
     * NOTE for data preparation: Prepare both Domain and Name with values which
     * don't exist in AccountMaster so that DUNS used in Account match is purely
     * from input
     *
     * @param caseIdx
     * @param partialKeys:
     *            Guaranteed to be prioritized
     */
    @Test(groups = "functional", priority = 3, dataProvider = "allKeysComboPrioritized", enabled = true)
    private void testMatchedPairWithHighPriKeyMatch(Integer caseIdx, List<String> partialKeys) {
        List<String> fullKeys = Arrays.asList(ACCOUNT_KEYS_PRIORITIZED);
        log.info("CaseIdx: {} (Out of 31)   Full Key: {}   Partial Key: {}", caseIdx, String.join(",", fullKeys),
                String.join(",", partialKeys));
        // Use different tenant for each test case because data provider is set
        // with parallel execution
        String tenantId = AccountMatchCorrectnessTestNG.class.getSimpleName() + "_" + UUID.randomUUID().toString();
        Tenant tenant = new Tenant(tenantId);

        // Data schema: ID_SFDC, ID_MKTO, ID_ELOQUA, Name, Domain, Country,
        // State, DUNS (ID_ELOQUA, Country, State are not used in this test;
        // DUNS needs to be real DUNS otherwise LDC match will not return
        // DUNS to Account match)
        List<Object> baseData1 = Arrays.asList("sfdc_id_1", "mkto_id_1", null, "fakename1", "fakedomain1.com", null,
                null, "060902413");
        List<Object> baseData2 = Arrays.asList("sfdc_id_2", "mkto_id_2", null, "fakename2", "fakedomain2.com", null,
                null, "884745530");

        
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
        // FIXME: If only bump version without change tenant, will hit NPE in
        // EntityAssociateServiceImpl
        // testEntityMatchService.bumpVersion(tenantId);
        tenantId = AccountMatchCorrectnessTestNG.class.getSimpleName() + "_" + UUID.randomUUID().toString();
        tenant = new Tenant(tenantId);
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
     * (try all the combo). So overlapped MatchKeys between 2 records is just
     * 2nd record's MatchKeys. Set 2nd record's HIGHEST priority MatchKey with
     * different input data with 1st record and all the other LOWER priority keys
     * with same input data. If 2nd record's HIGHEST priority MatchKey has
     * many-to-many relationship with Account, between match of 1st and 2nd records,
     * need to submit another match whose highest priority MatchKey has same value
     * as 2nd record. So that we can make sure when matching 2nd record, highest
     * priority key has conflict with 1st record
     *
     * eg. Record1: [sfdc_id_1, mkto_id_1, 060902413, fakedomain_1.com, fakename_1]
     * Record2: [mkto_id_2, 060902413, fakedomain_1.com] (ID_MKTO is highest priority key )
     *
     * NOTE for data preparation: Prepare both Domain and Name with values which
     * don't exist in AccountMaster so that DUNS used in Account match is purely
     * from input
     *
     * @param caseIdx
     * @param partialKeys:
     *            Guaranteed to be prioritized
     */
    @Test(groups = "functional", priority = 4, dataProvider = "allKeysComboPrioritized", enabled = true)
    private void testMatchedPairWithHighPriKeyMismatch(Integer caseIdx, List<String> partialKeys) {
        List<String> fullKeys = Arrays.asList(ACCOUNT_KEYS_PRIORITIZED);
        log.info("CaseIdx: {} (Out of 31)   Full Key: {}   Partial Key: {}", caseIdx, String.join(",", fullKeys),
                String.join(",", partialKeys));
        // Use different tenant for each test case because data provider is set
        // with parallel execution
        String tenantId = AccountMatchCorrectnessTestNG.class.getSimpleName() + "_" + UUID.randomUUID().toString();
        Tenant tenant = new Tenant(tenantId);

        // Data schema: ID_SFDC, ID_MKTO, ID_ELOQUA, Name, Domain, Country,
        // State, DUNS (ID_ELOQUA, Country, State are not used in this test;
        // DUNS needs to be real DUNS otherwise LDC match will not return
        // DUNS to Account match)
        List<Object> baseData1 = Arrays.asList("sfdc_id_1", "mkto_id_1", null, "fakename1", "fakedomain1.com", null,
                null, "060902413");
        List<Object> baseData2 = Arrays.asList("sfdc_id_2", "mkto_id_2", null, "fakename2", "fakedomain2.com", null,
                null, "884745530");

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
            matchAccount(data3, true, tenant, getEntityKeyMap(partialKeys));
        }
        runAndVerifyMatchPair(tenant, fullKeys, data1, partialKeys, data2, false);
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
     * Expectation: Submit 2 records to match service sequentially and should always return same EntityId
     *
     * eg.
     * Record 1: MatchKey group = ID_SFDC + DUNS, MatchKey = ID_SFDC + Name + Country
     * Record 2: MatchKey group = ID_MKTO + DUNS, MatchKey = ID_MKTO + Customer DUNS
     * Even if Record1 and Record2 don't have overlap in MatchKey,
     * since we design Name + Country in Record1 to match to Customer DUNS in Record2 by DnB,
     * they do overlapped MatchKeys in Account match
     *
     * NOTE: This test takes 3.5 mins and totally 916 test cases
     */
    @Test(groups = "functional", priority = 10, dataProvider = "exhaustiveKeysPairWithOverlap", enabled = false)
    private void testMatchedPairs(Integer caseIdx, List<String> keys1, List<String> keys2) {
        log.info("CaseIdx: {} (Out of 916)   Keys1: {}   Keys2: {}", caseIdx, String.join(",", keys1),
                String.join(",", keys2));

        // Use different tenant for each test case because data provider is set
        // with parallel execution
        String tenantId = AccountMatchCorrectnessTestNG.class.getSimpleName() + "_" + UUID.randomUUID().toString();
        Tenant tenant = new Tenant(tenantId);
        // Data and Fields are all the same, but MatchKey passed to MatchInput
        // are different
        List<Object> data = Arrays.asList("sfdc_id", "mkto_id", "eloqua_id", "google", "google.com", "usa", "ca",
                "060902413");

        runAndVerifyMatchPair(tenant, keys1, data, keys2, data, true);
    }

    private void runAndVerifyMatchPair(Tenant tenant, List<String> keys1, List<Object> data1, List<String> keys2,
            List<Object> data2, boolean isMatched) {
        Pair<MatchInput, MatchOutput> inputOutput1 = matchAccount(data1, true, tenant, getEntityKeyMap(keys1));
        MatchOutput output1 = inputOutput1.getRight();
        String entityId1 = verifyAndGetEntityId(output1);
        Assert.assertNotNull(entityId1, String.format("EntityId got null for Keys=%s   MatchInput=%s",
                String.join(",", keys1), JsonUtils.serialize(inputOutput1.getLeft())));

        Pair<MatchInput, MatchOutput> inputOutput2 = matchAccount(data2, true, tenant, getEntityKeyMap(keys2));
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

    private String lookupAccount(String sfdcId, String mktoId, String eloquaId, String name, String domain,
            String country, String state, String duns) {
        List<Object> data = Arrays.asList(sfdcId, mktoId, eloquaId, name, domain, country, state, duns);
        MatchOutput output = matchAccount(data, false, null, null).getRight();
        return verifyAndGetEntityId(output);
    }

    /*
     * make sure that match output has exactly one row that only contains entityId
     * column and return the entityId
     */
    private String verifyAndGetEntityId(@NotNull MatchOutput output) {
        Assert.assertNotNull(output);
        Assert.assertNotNull(output.getResult());
        Assert.assertEquals(output.getResult().size(), 1);
        OutputRecord record = output.getResult().get(0);
        Assert.assertNotNull(record);
        Assert.assertNotNull(record.getOutput());
        // check if output contains only entityId column
        Assert.assertEquals(output.getOutputFields(), Collections.singletonList(InterfaceName.EntityId.name()));
        Assert.assertEquals(record.getOutput().size(), 1);
        if (record.getOutput().get(0) != null) {
            Assert.assertTrue(record.getOutput().get(0) instanceof String);
        }
        return (String) record.getOutput().get(0);
    }

    /**
     * @param data
     * @param isAllocateMode
     * @param tenant:
     *            If null, use default tenant TEST_TENANT
     * @param entityKeyMap:
     *            If null, use getEntityKeyMap(Account)
     * @return
     */
    private Pair<MatchInput, MatchOutput> matchAccount(List<Object> data, boolean isAllocateMode, Tenant tenant,
            MatchInput.EntityKeyMap entityKeyMap) {
        String entity = BusinessEntity.Account.name();
        tenant = tenant == null ? TEST_TENANT : tenant;
        entityKeyMap = entityKeyMap == null ? getEntityKeyMap() : entityKeyMap;
        MatchInput input = prepareEntityMatchInput(tenant, entity, Collections.singletonMap(entity, entityKeyMap));
        input.setAllocateId(isAllocateMode); // Not take effect in this test
        entityMatchConfigurationService.setIsAllocateMode(isAllocateMode);
        input.setFields(Arrays.asList(FIELDS));
        input.setData(Collections.singletonList(data));
        return Pair.of(input, realTimeMatchService.match(input));
    }

    /*
     * helper to prepare basic MatchInput for entity match
     */
    private MatchInput prepareEntityMatchInput(@NotNull Tenant tenant, @NotNull String targetEntity,
            @NotNull Map<String, MatchInput.EntityKeyMap> entityKeyMaps) {
        MatchInput input = new MatchInput();

        input.setOperationalMode(OperationalMode.ENTITY_MATCH);
        input.setTenant(tenant);
        input.setTargetEntity(targetEntity);
        // only support this predefined selection for now
        input.setPredefinedSelection(ColumnSelection.Predefined.ID);
        input.setEntityKeyMaps(entityKeyMaps);
        input.setDataCloudVersion(currentDataCloudVersion);
        input.setSkipKeyResolution(true);
        input.setUseRemoteDnB(true);
        input.setUseDnBCache(true);
        input.setRootOperationUid(UUID.randomUUID().toString());

        return input;
    }

    private static MatchInput.EntityKeyMap getEntityKeyMap() {
        MatchInput.EntityKeyMap map = new MatchInput.EntityKeyMap();
        map.setSystemIdPriority(Arrays.asList(SYSTEM_ID_FIELDS));
        Map<MatchKey, List<String>> fieldMap = Arrays.stream(MATCH_KEY_FIELDS).collect(
                Collectors.toMap(matchKey -> matchKey, matchKey -> Collections.singletonList(matchKey.name())));
        fieldMap.put(MatchKey.SystemId, Arrays.asList(SYSTEM_ID_FIELDS));
        map.setKeyMap(fieldMap);
        return map;
    }

    private static MatchInput.EntityKeyMap getEntityKeyMap(List<String> keys) {
        MatchInput.EntityKeyMap map = new MatchInput.EntityKeyMap();
        Map<MatchKey, List<String>> keyMap = MatchKeyUtils.resolveKeyMap(keys);
        map.setKeyMap(keyMap);
        keys.forEach(key -> {
            switch (key) {
            case ID_SFDC:
            case ID_MKTO:
            case ID_ELOQUA:
                // Priority is same as their order in keys list
                map.addMatchKey(MatchKey.SystemId, key);
                break;
            default:
                break;
            }
        });
        map.setSystemIdPriority(keyMap.get(MatchKey.SystemId));
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

    @DataProvider(name = "allKeysComboPrioritized", parallel = true)
    private static Object[][] getAllKeysComboPrioritized() {
        List<String> accountKeys = Arrays.asList(ACCOUNT_KEYS_PRIORITIZED);
        List<List<String>> allCombos = getAllKeyGroupSubsets(accountKeys);
        return IntStream.range(0, allCombos.size()).mapToObj(idx -> new Object[] { idx, allCombos.get(idx) })
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
        for (List<String> groups1 : allSubSets)
            for (List<String> groups2 : allSubSets) {
                if (Collections.disjoint(groups1, groups2)) {
                    allDisjointSubsetPairs.add(Pair.of(groups1, groups2));

                }
            }
        return allDisjointSubsetPairs;
    }

    /**
     * With a list of MatchKey groups, return all combinations of MatchKeys
     * (deduped) accordingly
     *
     * eg. keyGroups = [ID_SFDC, DUNS, Name]
     * After DFS keys = [ID_SFDC, Customer DUNS, Name],
     *                  [ID_SFDC, Customer DUNS, Name, Country],
     *                  [ID_SFDC, Domain, Country, Name],
     *                  [ID_SFDC, Domain, Country, Name], (Deduped from [ID_SFDC, Domain, Country, Name, Country])
     *                  [ID_SFDC, Name, Country], (Deduped from [ID_SFDC, Name, Country, Name])
     *                  [ID_SFDC, Name, Country], (Deduped from [ID_SFDC, Name, Country, Name, Country])
     * After dedup keys = [ID_SFDC, Customer DUNS, Name],
     *                    [ID_SFDC, Customer DUNS, Name, Country],
     *                    [ID_SFDC, Domain, Country, Name],
     *                    [ID_SFDC, Name, Country]
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
                    // eg. [ID_SFDC, Name, Country, Name, Country] -> [ID_SFDC, Name, Country]
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
}
