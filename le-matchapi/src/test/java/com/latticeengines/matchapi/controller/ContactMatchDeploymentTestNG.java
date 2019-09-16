package com.latticeengines.matchapi.controller;

import static com.latticeengines.domain.exposed.datacloud.DataCloudConstants.ENTITY_ANONYMOUS_ID;
import static com.latticeengines.domain.exposed.datacloud.match.MatchKey.Domain;
import static com.latticeengines.domain.exposed.datacloud.match.MatchKey.Name;
import static com.latticeengines.domain.exposed.datacloud.match.MatchKey.SystemId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CompanyName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ContactName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Country;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CustomerAccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CustomerContactId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.Email;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.PhoneNumber;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.State;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import org.testng.util.Strings;

import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;
import com.latticeengines.domain.exposed.datacloud.match.InputBuffer;
import com.latticeengines.domain.exposed.datacloud.match.MatchConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.OperationalMode;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.matchapi.testframework.AdvancedMatchDeploymentTestNGBase;

// dpltc deploy -a matchapi,workflowapi,metadata,eai,modeling
public class ContactMatchDeploymentTestNG extends AdvancedMatchDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(ContactMatchDeploymentTestNG.class);

    private static final String TestId = "TestId";

    private static final int DEFAULT_TEST_ID_COL_INDEX = 0; // first col is testId
    private static final String[] DEFAULT_FIELDS = new String[] { TestId, //
            // contact fields (email used in both)
            CustomerContactId.name(), Email.name(), ContactName.name(), PhoneNumber.name(), //
            // account fields
            CustomerAccountId.name(), CompanyName.name(), Country.name(), State.name() };
    private static final String[] DEFAULT_MATCH_RESULT_FIELDS = ArrayUtils
            .addAll(new String[] { InterfaceName.EntityId.name(), InterfaceName.ContactId.name(),
                    InterfaceName.AccountId.name(), InterfaceName.LatticeAccountId.name() }, DEFAULT_FIELDS);
    private static final String[] NEW_ENTITY_FIELDS = new String[] { MatchConstants.ENTITY_NAME_FIELD,
            MatchConstants.ENTITY_ID_FIELD };

    private static final Object[][] EXISTING_DATA = { //
            // Google
            { //
                    "C0_01", //
                    "C_CID_01", "j.reese@google.com", "John Reese", "999-999-9999", //
                    "C_AID_01", "Google", "USA", "CA", //
            }, //
            { //
                    "C0_02", //
                    "C_CID_02", "h.finch@google.com", "Harold Finch", "888-888-8888", //
                    "C_AID_01", "Google", "USA", "CA", //
            }, //
            { //
                    "C0_03", //
                    "C_CID_03", "l.fusco@google.com", "Lionel Fusco", "777-777-7777", //
                    "C_AID_01", "Google", "USA", "CA", //
            }, //
            { //
                    "C0_04", //
                    "C_CID_04", "s.shaw@google.com", "Sameen Shaw", "666-666-6666", //
                    "C_AID_01", "Google", "USA", "CA", //
            }, //
            { //
                    "C0_05", //
                    "C_CID_05", "s.groves@google.com", "Samantha Groves", "555-555-5555", //
                    "C_AID_01", "Google", "USA", "CA", //
            }, //
               // Facebook
            { //
                    "C0_11", //
                    "C_CID_11", "j.greer@netflix.com", "John Greer", "444-444-4444", //
                    "C_AID_02", "Netflix", "USA", "CA", //
            }, //
            { //
                    "C0_12", //
                    "C_CID_12", "k.stanton@netflix.com", "Kara Stanton", "333-333-3333", //
                    "C_AID_02", "Netflix", "USA", "CA", //
            }, //
            { //
                    "C0_13", //
                    "C_CID_13", "j.lambert@netflix.com", "Jeremy Lambert", "222-222-2222", //
                    "C_AID_02", "Netflix", "USA", "CA", //
            }, //
    };
    // set of (set of customer contact IDs in the same account)
    private static final Set<Set<String>> EXISTING_CONTACT_GROUP = new HashSet<>();

    static {
        EXISTING_CONTACT_GROUP.add(Sets.newHashSet("C_CID_01", "C_CID_02", "C_CID_03", "C_CID_04", "C_CID_05"));
        EXISTING_CONTACT_GROUP.add(Sets.newHashSet("C_CID_11", "C_CID_12", "C_CID_13"));
    }

    // CustomerAccountId => AccountEntityId
    private final Map<String, String> accountEntityIdMap = new HashMap<>();
    // CustomerContactId => ContactEntityId
    private final Map<String, String> contactEntityIdMap = new HashMap<>();
    // AccountEntityId => Set(CustomerContactId)
    private final Map<String, Set<String>> contactsInAccount = new HashMap<>();

    @BeforeClass(groups = "deployment")
    @Override
    protected void init() {
        super.init();

        accountEntityIdMap.clear();
        contactEntityIdMap.clear();
    }

    @Test(groups = "deployment", priority = 1)
    private void populateExistingData() throws Exception {
        MatchInput input = prepareBulkMatchInput(prepareStringData("existing_data", DEFAULT_FIELDS, EXISTING_DATA));

        MatchCommand result = runAndVerifyBulkMatch(input, getPodId());
        Assert.assertNotNull(result);

        List<GenericRecord> matchResults = getRecords(getOutputPath(result), true, true);
        List<GenericRecord> newEntities = getRecords(getNewEntityPath(result), false, true);
        Assert.assertEquals(matchResults.size(), EXISTING_DATA.length,
                "Match result row count does not match existing data row count");
        Assert.assertFalse(newEntities.isEmpty(), "Should have newly allocated entities");

        logMatchResult(matchResults, getOutputPath(result));
        logNewEntities(newEntities, getNewEntityPath(result));
        // verify match result & new entities
        verifyMatchResultForExistingData(matchResults);
        // should only have two new accounts
        Set<String> accountEntityIds = verifyNewAccounts(newEntities, 2);

        Assert.assertEquals(new HashSet<>(accountEntityIdMap.values()), accountEntityIds,
                "AccountIds in match result should match ids in new entity list");

        int numAccounts = publishEntity(BusinessEntity.Account.name());
        int numContacts = publishEntity(BusinessEntity.Contact.name());
        Assert.assertEquals(numAccounts, 2, "Number of published accounts does not match the expected value");
        Assert.assertEquals(numContacts, EXISTING_DATA.length,
                "Number of published contacts does not match the expected value");
    }

    @Test(groups = "deployment", priority = 2, dataProvider = "contactImport1", dependsOnMethods = "populateExistingData")
    private void importData(String testGroupId, ContactBulkMatchTestCase[] testCases) throws Exception {
        // clear out staging (impact from other tests), existing data will be stored in
        // serving
        bumpStagingVersion();

        Object[][] data = new Object[testCases.length][];
        // testId => expected customer account/contact ID
        Map<String, String> expectedCAIds = new HashMap<>();
        Map<String, String> expectedCCIds = new HashMap<>();
        for (int i = 0; i < testCases.length; i++) {
            data[i] = testCases[i].data;
            String testId = (String) testCases[i].data[DEFAULT_TEST_ID_COL_INDEX];
            expectedCAIds.put(testId, testCases[i].expectedCustomerAccountId);
            expectedCCIds.put(testId, testCases[i].expectedCustomerContactId);
        }
        boolean hasNewAccount = expectedCAIds.values().stream().anyMatch(Objects::isNull);

        MatchInput input = prepareBulkMatchInput(
                prepareStringData(String.format("contact_import_1_group_%s", testGroupId), DEFAULT_FIELDS, data));

        MatchCommand result = runAndVerifyBulkMatch(input, getPodId());
        Assert.assertNotNull(result);

        List<GenericRecord> matchResults = getRecords(getOutputPath(result), true, true);
        List<GenericRecord> newEntities = getRecords(getNewEntityPath(result), false, true);

        logMatchResult(matchResults, getOutputPath(result));
        logNewEntities(newEntities, getNewEntityPath(result));

        Assert.assertEquals(matchResults.size(), data.length,
                "Match result row count does not match existing data row count");
        if (hasNewAccount) {
            Assert.assertFalse(newEntities.isEmpty(), "Should have newly allocated accounts");
            verifyNewAccounts(newEntities, -1);
        } else {
            Assert.assertTrue(newEntities.isEmpty(), "Should not have newly allocated accounts");
        }

        Pair<Map<String, String>, Map<String, String>> entityIdMaps = verifyMatchResultForImportData(matchResults);
        Map<String, String> aids = entityIdMaps.getLeft();
        Map<String, String> cids = entityIdMaps.getRight();
        verifyMatchedIds(BusinessEntity.Account.name(), aids, accountEntityIdMap, expectedCAIds);
        verifyMatchedIds(BusinessEntity.Contact.name(), cids, contactEntityIdMap, expectedCCIds);
    }

    @DataProvider(name = "contactImport1")
    private Object[][] contactImport1() {
        return new Object[][] { //
                { //
                        "grp1", //
                        new ContactBulkMatchTestCase[] { //
                                new ContactBulkMatchTestCase( //
                                        new Object[] { //
                                                "C1_01", //
                                                "C_CID_01", "j.reese@google.com", "John Reese", "999-999-9999", //
                                                "C_AID_01", "Google", "USA", "CA", //
                                        }, "C_AID_01", "C_CID_01" //
                                ), // match with CustomerAccountId & CustomerContactId
                                new ContactBulkMatchTestCase( //
                                        new Object[] { //
                                                "C1_02", //
                                                null, "j.reese@google.com", null, null, //
                                                null, "Google", "USA", "CA", //
                                        }, "C_AID_01", "C_CID_01" //
                                ), // match with Name/Country for account & AccountEntityId + Email for contact
                                new ContactBulkMatchTestCase( //
                                        new Object[] { //
                                                "C1_03", //
                                                null, "l.fusco123@google.com", "Lionel Fusco", "777-777-7777", //
                                                "C_AID_01", "Google Inc.", "USA", "CA", //
                                        }, "C_AID_01", "C_CID_03" //
                                ), // CustomerAccountId for account & AccountEntityId + N + P for contact
                                new ContactBulkMatchTestCase( //
                                        new Object[] { //
                                                "C1_04", //
                                                null, null, "Samantha Groves", "555-555-5555", //
                                                null, "Lyft", "USA", "CA", //
                                        }, null, null //
                                ), // New account & New contact (no CustomerContactId, so AccountEntityId + N+P
                                   // does not match even though N+P are the same
                                new ContactBulkMatchTestCase( //
                                        new Object[] { //
                                                "C1_05", //
                                                "C_CID_05", "s.groves@uber.com", "Samantha Groves", "555-555-5555", //
                                                null, "Uber", "USA", "CA", //
                                        }, null, "C_CID_05" //
                                ), // New account & CustomerContactId for contact (change company)
                                new ContactBulkMatchTestCase( //
                                        new Object[] { //
                                                "C1_06", //
                                                null, "bear@google.com", null, null, //
                                                null, "Google", "USA", "CA", //
                                        }, "C_AID_01", null //
                                ), // match with Name/Country for account & new contact
                                new ContactBulkMatchTestCase( //
                                        new Object[] { //
                                                "C1_07", //
                                                "C_CID_999", "samaritan@netflix.com", null, null, //
                                                null, "Netflix", "USA", "CA", //
                                        }, "C_AID_02", null //
                                ), // match with Name/Country for account & new contact
                                new ContactBulkMatchTestCase( //
                                        new Object[] { //
                                                "C1_08", //
                                                null, null, "John Doe", "000-000-0000", //
                                                null, "Netflix", "USA", "CA", //
                                        }, "C_AID_02", null //
                                ), // match with Name/Country for account & new contact
                                new ContactBulkMatchTestCase( //
                                        new Object[] { //
                                                "C1_09", //
                                                null, null, null, null, //
                                                null, "Netflix", "USA", "CA", //
                                        }, "C_AID_02", ENTITY_ANONYMOUS_ID //
                                ), // match with Name/Country for account & anonymous contact
                                new ContactBulkMatchTestCase( //
                                        new Object[] { //
                                                "C1_10", //
                                                null, null, null, null, //
                                                null, null, null, null //
                                        }, ENTITY_ANONYMOUS_ID, ENTITY_ANONYMOUS_ID //
                                ), // anonymous account & contact
                                new ContactBulkMatchTestCase( //
                                        new Object[] { //
                                                "C1_11", //
                                                "C_CID_13", null, "Jeremy Lambert", "222-222-2222", //
                                                null, null, null, null //
                                        }, ENTITY_ANONYMOUS_ID, "C_CID_13" //
                                ), // anonymous account & match with CustomerContactId for contact (note that N+P
                                   // does not match because existing data all have account info, so only
                                   // AccountEntityId+N+P mapping is setup, we have to use CustomerContactId to
                                   // link, we will be able to match with N+P after the mapping is setup though)
                        }, //
                }, //
        };
    }

    // testIdToEntityIds: testId => entityId
    // customerIdToExistingEntityIds: customerId => entityId
    // expectedCustomerIds: testId => customerId (null means new entity)
    private void verifyMatchedIds(@NotNull String entity, @NotNull Map<String, String> testIdToEntityIds,
            @NotNull Map<String, String> customerIdToExistingEntityIds,
            @NotNull Map<String, String> expectedCustomerIds) {
        Set<String> existingEntityIds = new HashSet<>(customerIdToExistingEntityIds.values());
        for (Map.Entry<String, String> entry : testIdToEntityIds.entrySet()) {
            String testId = entry.getKey();
            String entityId = entry.getValue();
            Assert.assertTrue(expectedCustomerIds.containsKey(testId),
                    String.format("TestId=%s should be in expected customer %s id map", testId, entity));
            String expectedCId = expectedCustomerIds.get(testId);
            if (ENTITY_ANONYMOUS_ID.equals(expectedCId)) {
                Assert.assertEquals(entityId, ENTITY_ANONYMOUS_ID, String.format("Should be an anonymous %s", entity));
            } else if (expectedCId != null) {
                String expectedEntityId = customerIdToExistingEntityIds.get(expectedCId);
                // existing entity
                Assert.assertTrue(existingEntityIds.contains(entityId),
                        String.format("EntityId=%s for TestId=%s should be an existing %s", entityId, testId, entity));
                Assert.assertEquals(entityId, expectedEntityId,
                        String.format("%sId for TestId=%s does not match the expected ID", entity, testId));
            } else {
                // new entity
                Assert.assertFalse(existingEntityIds.contains(entityId),
                        String.format("EntityId=%s for TestId=%s should be a new %s", entityId, testId, entity));
            }
        }
    }

    // return [ Map<TestId, AccountEntityId>, Map<TestId, ContactEntityId> ]
    private Pair<Map<String, String>, Map<String, String>> verifyMatchResultForImportData(
            @NotNull List<GenericRecord> records) {
        // testId => account/contact entity ID
        Map<String, String> aids = new HashMap<>();
        Map<String, String> cids = new HashMap<>();
        for (int i = 0; i < records.size(); i++) {
            GenericRecord record = records.get(i);
            verifyEntityIdFields(record, i);
            String contactEntityId = getStrValue(record, InterfaceName.ContactId.name());
            String accountEntityId = getStrValue(record, InterfaceName.AccountId.name());
            String testId = getStrValue(record, TestId);

            aids.put(testId, accountEntityId);
            cids.put(testId, contactEntityId);
        }
        return Pair.of(aids, cids);
    }

    private void verifyMatchResultForExistingData(@NotNull List<GenericRecord> records) {
        Assert.assertEquals(records.size(), EXISTING_DATA.length,
                "Match result should have the same length as existing data");
        for (int i = 0; i < records.size(); i++) {
            GenericRecord record = records.get(i);
            String contactEntityId = getStrValue(record, InterfaceName.ContactId.name());
            String accountEntityId = getStrValue(record, InterfaceName.AccountId.name());
            String latticeAccountId = getStrValue(record, InterfaceName.LatticeAccountId.name());
            verifyEntityIdFields(record, i);
            Assert.assertNotNull(latticeAccountId,
                    String.format("Should have non-null LatticeAccountId for existing data. Index=%d, Record=%s", i,
                            toString(record, DEFAULT_MATCH_RESULT_FIELDS)));

            String customerAccountId = getStrValue(record, CustomerAccountId.name());
            String customerContactId = getStrValue(record, CustomerContactId.name());
            accountEntityIdMap.put(customerAccountId, accountEntityId);
            contactEntityIdMap.put(customerContactId, contactEntityId);
            contactsInAccount.putIfAbsent(accountEntityId, new HashSet<>());
            contactsInAccount.get(accountEntityId).add(customerContactId);
        }
    }

    private void verifyEntityIdFields(@NotNull GenericRecord record, int idx) {
        String entityId = getStrValue(record, InterfaceName.EntityId.name());
        String contactEntityId = getStrValue(record, InterfaceName.ContactId.name());
        String accountEntityId = getStrValue(record, InterfaceName.AccountId.name());
        Assert.assertNotNull(entityId,
                String.format("Should have non-null EntityId for existing data. Index=%d, Record=%s", idx,
                        toString(record, DEFAULT_MATCH_RESULT_FIELDS)));
        Assert.assertNotNull(contactEntityId,
                String.format("Should have non-null ContactId for existing data. Index=%d, Record=%s", idx,
                        toString(record, DEFAULT_MATCH_RESULT_FIELDS)));
        Assert.assertNotNull(accountEntityId,
                String.format("Should have non-null AccountId for existing data. Index=%d, Record=%s", idx,
                        toString(record, DEFAULT_MATCH_RESULT_FIELDS)));
        Assert.assertEquals(contactEntityId, entityId, "ContactId should be the same as EntityId");
    }

    // if expectedSize = -1, do not verify size
    private Set<String> verifyNewAccounts(@NotNull List<GenericRecord> records, int expectedSize) {
        if (expectedSize != -1) {
            Assert.assertEquals(records.size(), expectedSize,
                    "Number of newly allocated accounts does not match the expected number");
        }
        Set<String> newEntityIds = new HashSet<>();
        for (int i = 0; i < records.size(); i++) {
            GenericRecord record = records.get(i);
            String entityId = getStrValue(record, MatchConstants.ENTITY_ID_FIELD);
            String entityName = getStrValue(record, MatchConstants.ENTITY_NAME_FIELD);
            Assert.assertNotNull(entityId,
                    String.format("Should have non-null EntityId for newly allocated account. Index=%d, Record=%s", i,
                            toString(record, NEW_ENTITY_FIELDS)));
            Assert.assertEquals(entityName, BusinessEntity.Account.name(),
                    String.format("Got newly allocated entity that is not Account at index=%d", i));
            newEntityIds.add(entityId);
        }
        return newEntityIds;
    }

    private void logMatchResult(@NotNull List<GenericRecord> records, @NotNull String path) {
        log.info("MatchResults(path={},size={})", path, records.size());
        records.stream().map(record -> toString(record, DEFAULT_MATCH_RESULT_FIELDS)).forEach(log::info);
        log.info("===END===");
    }

    private void logNewEntities(@NotNull List<GenericRecord> records, @NotNull String path) {
        log.info("NewEntities(path={},size={})", path, records.size());
        records.stream().map(record -> toString(record, NEW_ENTITY_FIELDS)).forEach(log::info);
        log.info("===END===");
    }

    private String toString(@NotNull GenericRecord record, @NotNull String[] fields) {
        String[] tokens = Arrays.stream(fields).map(field -> String.format("%s=%s", field, getStrValue(record, field)))
                .toArray(String[]::new);
        return Strings.join(",", tokens);
    }

    private String getOutputPath(MatchCommand command) {
        return command.getResultLocation();
    }

    private String getNewEntityPath(MatchCommand command) {
        return getOutputPath(command).replace("Output", "NewEntities");
    }

    private MatchInput prepareBulkMatchInput(InputBuffer buffer) {
        MatchInput input = new MatchInput();
        input.setTenant(testTenant);
        input.setDataCloudVersion(currentDataCloudVersion);
        input.setPredefinedSelection(ColumnSelection.Predefined.ID);
        input.setFields(Arrays.asList(DEFAULT_FIELDS));
        input.setSkipKeyResolution(true);
        input.setOperationalMode(OperationalMode.ENTITY_MATCH);
        input.setTargetEntity(BusinessEntity.Contact.name());
        input.setAllocateId(true);
        input.setOutputNewEntities(true);
        input.setEntityKeyMaps(getDefaultKeyMaps());
        input.setInputBuffer(buffer);
        input.setUseDnBCache(true);
        input.setUseRemoteDnB(true);
        return input;
    }

    /*
     * TODO merge default key map with ContactMatchCorrectnessTestNG after
     * correctness tests are finalized
     */

    private static Map<String, MatchInput.EntityKeyMap> getDefaultKeyMaps() {
        Map<String, MatchInput.EntityKeyMap> keyMaps = new HashMap<>();
        keyMaps.put(BusinessEntity.Account.name(), getDefaultAccountKeyMap());
        keyMaps.put(BusinessEntity.Contact.name(), getDefaultContactKeyMap());
        return keyMaps;
    }

    private static MatchInput.EntityKeyMap getDefaultAccountKeyMap() {
        MatchInput.EntityKeyMap map = new MatchInput.EntityKeyMap();
        map.addMatchKey(SystemId, CustomerAccountId.name());
        map.addMatchKey(Name, CompanyName.name());
        // only use email for account domain for now, TODO add more later
        map.addMatchKey(Domain, MatchKey.Email.name());
        map.addMatchKey(MatchKey.Country, MatchKey.Country.name());
        map.addMatchKey(MatchKey.State, MatchKey.State.name());
        return map;
    }

    private static MatchInput.EntityKeyMap getDefaultContactKeyMap() {
        MatchInput.EntityKeyMap map = new MatchInput.EntityKeyMap();
        map.addMatchKey(SystemId, CustomerContactId.name());
        map.addMatchKey(MatchKey.Email, MatchKey.Email.name());
        map.addMatchKey(Name, ContactName.name());
        map.addMatchKey(MatchKey.PhoneNumber, MatchKey.PhoneNumber.name());
        return map;
    }

    private class ContactBulkMatchTestCase {
        Object[] data;
        // null means new account/contact
        String expectedCustomerAccountId;
        String expectedCustomerContactId;

        ContactBulkMatchTestCase(Object[] data, String expectedCustomerAccountId,
                String expectedCustomerContactId) {
            this.data = data;
            this.expectedCustomerAccountId = expectedCustomerAccountId;
            this.expectedCustomerContactId = expectedCustomerContactId;
        }
    }
}
