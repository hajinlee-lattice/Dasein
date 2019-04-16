package com.latticeengines.datacloud.match.service.impl;

import static com.latticeengines.domain.exposed.datacloud.match.MatchKey.Country;
import static com.latticeengines.domain.exposed.datacloud.match.MatchKey.Domain;
import static com.latticeengines.domain.exposed.datacloud.match.MatchKey.Email;
import static com.latticeengines.domain.exposed.datacloud.match.MatchKey.Name;
import static com.latticeengines.domain.exposed.datacloud.match.MatchKey.PhoneNumber;
import static com.latticeengines.domain.exposed.datacloud.match.MatchKey.State;
import static com.latticeengines.domain.exposed.datacloud.match.MatchKey.SystemId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.AccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CompanyName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.ContactName;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CustomerAccountId;
import static com.latticeengines.domain.exposed.metadata.InterfaceName.CustomerContactId;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.testframework.EntityMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.testframework.service.impl.SimpleRetryListener;

/**
 * This test is mainly focused on Contact match with AllocateId mode
 */
@Listeners({ SimpleRetryListener.class })
public class ContactMatchCorrectnessTestNG extends EntityMatchFunctionalTestNGBase {

    private static final AtomicInteger TESTCASE_COUNTER = new AtomicInteger(0);

    private static final Logger log = LoggerFactory.getLogger(ContactMatchCorrectnessTestNG.class);

    // [ contact fields + account fields ]
    private static final String[] DEFAULT_FIELDS = new String[] {
            // contact fields (email used in both)
            CustomerContactId.name(), Email.name(), ContactName.name(), PhoneNumber.name(),
            // account fields
            CustomerAccountId.name(), CompanyName.name(), Country.name(), State.name() };

    @Test(groups = "functional", dataProvider = "basicContactMatch", retryAnalyzer = SimpleRetryListener.class)
    private void testBasicContactMatch(ContactMatchTestCase testCase) {
        Tenant tenant = newTestTenant();

        String contactEntityId = null;
        String accountEntityId = null;
        if (testCase.existingData != null) {
            // populate existing data
            Pair<MatchInput, MatchOutput> result = matchContactWithDefaultFields(tenant, testCase.existingData);
            MatchOutput output = result.getRight();
            Assert.assertNotNull(output,
                    String.format("MatchOutput of existing data for test case %s should not be null", testCase));
            contactEntityId = verifyAndGetEntityId(output, InterfaceName.ContactId.name());
            accountEntityId = getColumnValue(output, AccountId.name());
        }

        // match import data
        Pair<MatchInput, MatchOutput> result = matchContactWithDefaultFields(tenant, testCase.importData);
        MatchOutput output = result.getRight();
        Assert.assertNotNull(output,
                String.format("MatchOutput of import data for test case %s should not be null", testCase));
        String importContactEntityId = verifyAndGetEntityId(output, InterfaceName.ContactId.name());
        String importAccountEntityId = getColumnValue(output, AccountId.name());
        verifyEntityId(BusinessEntity.Contact.name(), importContactEntityId, contactEntityId, testCase.contactStatus,
                testCase);
        verifyEntityId(BusinessEntity.Account.name(), importAccountEntityId, accountEntityId, testCase.accountStatus,
                testCase);
    }

    @DataProvider(name = "basicContactMatch", parallel = true)
    private Object[][] basicContactMatchTestData() {
        return new Object[][] { //
                // account/contact info are exactly the same as existing data
                { new ContactMatchTestCase( //
                        new String[] { "C_CID_1", "j.reese@google.com", "John Reese", "999-999-9999", "C_AID_1",
                                "Google", "USA", "CA" }, //
                        new String[] { "C_CID_1", "j.reese@google.com", "John Reese", "999-999-9999", "C_AID_1",
                                "Google", "USA", "CA" }, //
                        EntityMatchStatus.MERGE_EXISTING, EntityMatchStatus.MERGE_EXISTING) }, //
                // match to contact/account with customer contact/account ID
                { new ContactMatchTestCase( //
                        new String[] { "C_CID_1", "j.reese@google.com", "John Reese", "999-999-9999", "C_AID_1",
                                "Google", "USA", "CA" }, //
                        new String[] { "C_CID_1", null, null, null, "C_AID_1", null, null, null }, //
                        EntityMatchStatus.MERGE_EXISTING, EntityMatchStatus.MERGE_EXISTING) }, //
                // no existing data, allocate new account & contact
                { new ContactMatchTestCase( //
                        null, //
                        new String[] { "C_CID_1", null, "John Reese", "999-999-9999", null, null, null, null }, //
                        EntityMatchStatus.ANONYMOUS, EntityMatchStatus.ALLOCATE_NEW) }, //
                // import data has NO account info, existing data has account info so email & name/phone cannot match
                { new ContactMatchTestCase( //
                        new String[] { "C_CID_1", "j.reese@google.com", "John Reese", "999-999-9999", "C_AID_1",
                                "Google", "USA", "CA" }, //
                        new String[] { null, null, "John Reese", "999-999-9999", null, null, null, null }, //
                        EntityMatchStatus.ANONYMOUS, EntityMatchStatus.ALLOCATE_NEW) }, //
                // match to account with customer account ID, match to contact with accountEntityId + Name + Phone
                { new ContactMatchTestCase( //
                        new String[] { "C_CID_1", "j.reese@google.com", "John Reese", "999-999-9999", "C_AID_1",
                                "Google", "USA", "CA" }, //
                        new String[] { null, null, "John Reese", "999-999-9999", "C_AID_1", null, null, null }, //
                        EntityMatchStatus.MERGE_EXISTING, EntityMatchStatus.MERGE_EXISTING) }, //
                // import data has DIFFERENT account info, therefore even email/name/phone are the same, does not
                // match to existing contact. Allocate both new account & contact
                { new ContactMatchTestCase( //
                        new String[] { "C_CID_1", "j.reese@google.com", "John Reese", "999-999-9999", "C_AID_1",
                                "Google", "USA", "CA" }, //
                        new String[] { null, "j.reese@google.com", "John Reese", "999-999-9999", "C_AID_2", null, null, null }, //
                        EntityMatchStatus.ALLOCATE_NEW, EntityMatchStatus.ALLOCATE_NEW) }, //
                // match to contact with customer contact ID, and merge with different account info (change company)
                { new ContactMatchTestCase( //
                        new String[] { "C_CID_1", "j.reese@gmail.com", "John Reese", "999-999-9999", "C_AID_1",
                                "Google", "USA", "CA" }, //
                        new String[] { "C_CID_1", "j.reese@gmail.com", "John Reese", "999-999-9999", "C_AID_2", "Facebook", null, null }, //
                        EntityMatchStatus.ALLOCATE_NEW, EntityMatchStatus.MERGE_EXISTING) }, //
                // match to contact with customer contact ID, create new account since existing data has no account info
                { new ContactMatchTestCase( //
                        new String[] { "C_CID_1", null, "John Reese", "999-999-9999", null, null, null, null }, //
                        new String[] { "C_CID_1", "j.reese@google.com", "John Reese", "999-999-9999", "C_AID_2", null, null, null }, //
                        EntityMatchStatus.ALLOCATE_NEW, EntityMatchStatus.MERGE_EXISTING) }, //
                // match to contact with account entity ID + email, has conflict in contact ID
                // allocate new contact but match to existing account
                { new ContactMatchTestCase( //
                        new String[] { "C_CID_1", "j.reese@google.com", null, null, "C_AID_1",
                                "Google", "USA", "CA" }, //
                        new String[] { "C_CID_2", "j.reese@google.com", "John Reese", "999-999-9999", "C_AID_1", null, null, null }, //
                        EntityMatchStatus.MERGE_EXISTING, EntityMatchStatus.ALLOCATE_NEW) }, //
                // no contact info but has account info => anonymous contact and new account
                { new ContactMatchTestCase( //
                        null, //
                        new String[] { null, null, null, null, "C_AID_1",
                                "Google", "USA", "CA" }, //
                        EntityMatchStatus.ALLOCATE_NEW, EntityMatchStatus.ANONYMOUS) }, //
                // no contact & account info => anonymous account and contact
                { new ContactMatchTestCase( //
                        null, //
                        new String[] { null, null, null, null, null, null, null, null }, //
                        EntityMatchStatus.ALLOCATE_NEW, EntityMatchStatus.ANONYMOUS) }, //
        };
    }

    private void verifyEntityId(String entity, String importEntityId, String existingEntityId, EntityMatchStatus status,
            ContactMatchTestCase testCase) {
        if (status == EntityMatchStatus.ALLOCATE_NEW) {
            Assert.assertNotEquals(importEntityId, existingEntityId,
                    String.format(
                            "%sEntityId for import data (%s) should not be the same as existing data for test case %s",
                            entity, importEntityId, testCase));
        } else if (status == EntityMatchStatus.MERGE_EXISTING) {
            Assert.assertEquals(importEntityId, existingEntityId,
                    String.format(
                            "%sEntityId for import data (%s) should be the same as existing data (%s) for test case %s",
                            entity, importEntityId, existingEntityId, testCase));
        } else if (status == EntityMatchStatus.ANONYMOUS) {
            Assert.assertEquals(importEntityId, DataCloudConstants.ENTITY_ANONYMOUS_ID,
                    String.format("%sEntityId for import data (%s) should be anonymous for test case %s", entity,
                            importEntityId, testCase));
        }
    }

    // TODO implement full correctness test cases

    /*
     * contact match using default test fields in allocateId mode
     */
    private Pair<MatchInput, MatchOutput> matchContactWithDefaultFields(Tenant tenant, String[] data) {
        String entity = BusinessEntity.Contact.name();
        MatchInput input = prepareEntityMatchInput(tenant, entity, getDefaultKeyMaps());
        input.setFields(Arrays.asList(DEFAULT_FIELDS));
        input.setData(Collections.singletonList(Arrays.asList(data)));
        entityMatchConfigurationService.setIsAllocateMode(true);
        input.setAllocateId(true);
        input.setPublicDomainAsNormalDomain(false);
        return Pair.of(input, realTimeMatchService.match(input));
    }

    @Override
    protected List<String> getExpectedOutputColumns() {
        return Arrays.asList(InterfaceName.EntityId.name(), InterfaceName.ContactId.name(),
                InterfaceName.AccountId.name(), InterfaceName.LatticeAccountId.name());
    }

    @Override
    protected Logger getLogger() {
        return log;
    }

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
        map.addMatchKey(Domain, Email.name());
        map.addMatchKey(Country, Country.name());
        map.addMatchKey(State, State.name());
        return map;
    }

    private static MatchInput.EntityKeyMap getDefaultContactKeyMap() {
        MatchInput.EntityKeyMap map = new MatchInput.EntityKeyMap();
        map.addMatchKey(SystemId, CustomerContactId.name());
        map.addMatchKey(Email, Email.name());
        map.addMatchKey(Name, ContactName.name());
        map.addMatchKey(PhoneNumber, PhoneNumber.name());
        return map;
    }

    private class ContactMatchTestCase {
        int idx = TESTCASE_COUNTER.getAndIncrement();
        String[] fields;
        String[] existingData;
        String[] importData;
        EntityMatchStatus accountStatus;
        EntityMatchStatus contactStatus;

        ContactMatchTestCase(String[] existingData, String[] importData, EntityMatchStatus accountStatus,
                EntityMatchStatus contactStatus) {
            this.fields = DEFAULT_FIELDS;
            this.existingData = existingData;
            this.importData = importData;
            this.accountStatus = accountStatus;
            this.contactStatus = contactStatus;
        }

        ContactMatchTestCase(String[] fields, String[] existingData, String[] importData,
                EntityMatchStatus accountStatus, EntityMatchStatus contactStatus) {
            this.fields = fields;
            this.existingData = existingData;
            this.importData = importData;
            this.accountStatus = accountStatus;
            this.contactStatus = contactStatus;
        }

        @Override public String toString() {
            return "ContactMatchTestCase{" + "testCase=" + idx + ", fields=" + Arrays.toString(fields) + ", existingData="
                    + Arrays.toString(existingData) + ", importData=" + Arrays.toString(importData) + ", accountStatus="
                    + accountStatus + ", contactStatus=" + contactStatus + '}';
        }
    }

    private enum EntityMatchStatus {
        ALLOCATE_NEW, // allocate a new entity
        MERGE_EXISTING, // merge to an existing entity
        ANONYMOUS // match to anonymous entity
    }
}
