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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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
import com.latticeengines.testframework.service.impl.SimpleRetryAnalyzer;
import com.latticeengines.testframework.service.impl.SimpleRetryListener;

/**
 * This test is mainly focused on Contact match with AllocateId mode
 */
@Listeners({ SimpleRetryListener.class })
public class ContactMatchCorrectnessTestNG extends EntityMatchFunctionalTestNGBase {

    private static final AtomicInteger TESTCASE_COUNTER = new AtomicInteger(0);

    private static final Logger log = LoggerFactory.getLogger(ContactMatchCorrectnessTestNG.class);

    private static final String[] DEFAULT_FIELDS = new String[] {
            // contact fields (email used in both)
            CustomerContactId.name(), Email.name(), ContactName.name(), PhoneNumber.name(),
            // account fields
            CustomerAccountId.name(), CompanyName.name(), Country.name(), State.name() };

    @Test(groups = "functional", dataProvider = "basicContactMatch", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testBasicContactMatch(ContactMatchTestCase testCase) {
        matchAndVerify(testCase);
    }

    @DataProvider(name = "basicContactMatch", parallel = true)
    private Object[][] basicContactMatchTestData() {
        return new Object[][] { //
                // account/contact info are exactly the same as existing data
                { new ContactMatchTestCase( //
                        new String[][] { { "C_CID_1", "j.reese@google.com", "John Reese", "999-999-9999", "C_AID_1",
                                "Google", "USA", "CA" } }, //
                        new String[] { "C_CID_1", "j.reese@google.com", "John Reese", "999-999-9999", "C_AID_1",
                                "Google", "USA", "CA" }, //
                        EntityMatchStatus.MERGE_EXISTING, EntityMatchStatus.MERGE_EXISTING) }, //
                // match to contact/account with customer contact/account ID
                { new ContactMatchTestCase( //
                        new String[][] { { "C_CID_1", "j.reese@google.com", "John Reese", "999-999-9999", "C_AID_1",
                                "Google", "USA", "CA" } }, //
                        new String[] { "C_CID_1", null, null, null, "C_AID_1", null, null, null }, //
                        EntityMatchStatus.MERGE_EXISTING, EntityMatchStatus.MERGE_EXISTING) }, //
                // no existing data, allocate new account & contact
                { new ContactMatchTestCase( //
                        null, //
                        new String[] { "C_CID_1", null, "John Reese", "999-999-9999", null, null, null, null }, //
                        EntityMatchStatus.ANONYMOUS, EntityMatchStatus.ALLOCATE_NEW) }, //
                // import data has NO account info, existing data has account info
                // since we create both acct/name/phone & name/phone lookup mapping, import data
                // can be matched to existing one
                { new ContactMatchTestCase( //
                        new String[][] { { "C_CID_1", "j.reese@google.com", "John Reese", "999-999-9999", "C_AID_1",
                                "Google", "USA", "CA" } }, //
                        new String[] { null, null, "John Reese", "999-999-9999", null, null, null, null }, //
                        EntityMatchStatus.ANONYMOUS, EntityMatchStatus.MERGE_EXISTING) }, //
                // match to account with customer account ID, match to contact with accountEntityId + Name + Phone
                { new ContactMatchTestCase( //
                        new String[][] { { "C_CID_1", "j.reese@google.com", "John Reese", "999-999-9999", "C_AID_1",
                                "Google", "USA", "CA" } }, //
                        new String[] { null, null, "John Reese", "999-999-9999", "C_AID_1", null, null, null }, //
                        EntityMatchStatus.MERGE_EXISTING, EntityMatchStatus.MERGE_EXISTING) }, //
                // import data has DIFFERENT account info, therefore even email/name/phone are the same, does not
                // match to existing contact. Allocate both new account & contact
                { new ContactMatchTestCase( //
                        new String[][] { { "C_CID_1", "j.reese@google.com", "John Reese", "999-999-9999", "C_AID_1",
                                "Google", "USA", "CA" } }, //
                        new String[] { null, "j.reese@google.com", "John Reese", "999-999-9999", "C_AID_2", null, null, null }, //
                        EntityMatchStatus.ALLOCATE_NEW, EntityMatchStatus.ALLOCATE_NEW) }, //
                // match to contact with customer contact ID, and merge with different account info (change company)
                { new ContactMatchTestCase( //
                        new String[][] { { "C_CID_1", "j.reese@gmail.com", "John Reese", "999-999-9999", "C_AID_1",
                                "Google", "USA", "CA" } }, //
                        new String[] { "C_CID_1", "j.reese@gmail.com", "John Reese", "999-999-9999", "C_AID_2", "Facebook", null, null }, //
                        EntityMatchStatus.ALLOCATE_NEW, EntityMatchStatus.MERGE_EXISTING) }, //
                // match to contact with customer contact ID, create new account since existing data has no account info
                { new ContactMatchTestCase( //
                        new String[][] { { "C_CID_1", null, "John Reese", "999-999-9999", null, null, null, null } }, //
                        new String[] { "C_CID_1", "j.reese@google.com", "John Reese", "999-999-9999", "C_AID_2", null, null, null }, //
                        EntityMatchStatus.ALLOCATE_NEW, EntityMatchStatus.MERGE_EXISTING) }, //
                // match to contact with account entity ID + email, has conflict in contact ID
                // allocate new contact but match to existing account
                { new ContactMatchTestCase( //
                        new String[][] {
                                { "C_CID_1", "j.reese@google.com", null, null, "C_AID_1", "Google", "USA", "CA" } }, //
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

    @Test(groups = "functional", dataProvider = "existedAccountContact", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testMatchExistedAccountContact(ContactMatchTestCase testCase) {
        testCase.setAccountStatus(EntityMatchStatus.MERGE_EXISTING);
        testCase.setContactStatus(EntityMatchStatus.MERGE_EXISTING);
        matchAndVerify(testCase);
    }

    // Not cover any case related to anonymous, public domain & multi-domain
    // Schema: CustomerContactId, Email, ContactName, PhoneNumber,
    // CustomerAccountId, CompanyName, Country, State
    @DataProvider(name = "existedAccountContact", parallel = true)
    private Object[][] existedAccountContactTestData() {
        return new Object[][] { //
                // Contact: CCID; Account: CAID (Won't cover all Account match
                // key cases as matched AccountId doesn't take effect in Contact
                // match if CustomerContactId is provided)
                { new ContactMatchTestCase( //
                        new String[][] { { "C_CID_1", null, null, null, "C_AID_1", "Google", "USA", "CA" } }, //
                        new String[] { "C_CID_1", "j.reese@google.com", "John Reese", "999-999-9999", "C_AID_1",
                                null, null, null }) }, //
                { new ContactMatchTestCase( //
                        new String[][] { { "C_CID_1", "l.torvalds@google.com", "Linus Torvalds", "111-111-1111",
                                "C_AID_1", "Google", "USA", "CA" } }, //
                        new String[] { "C_CID_1", "j.reese@google.com", "John Reese", "999-999-9999", "C_AID_1",
                                "Facebook", "USA", "CA" }) }, //

                // Contact: Email + AID; Account: CAID
                { new ContactMatchTestCase( //
                        new String[][] { { null, "j.reese@google.com", null, null, "C_AID_1", null, null, null } }, //
                        new String[] { null, "j.reese@google.com", "John Reese", "999-999-9999", "C_AID_1", null, null,
                                null }) }, //
                { new ContactMatchTestCase( //
                        new String[][] { { null, "j.reese@google.com", "Linus Torvalds", "111-111-1111", "C_AID_1",
                                "Google", "USA", "CA" } }, //
                        new String[] { null, "j.reese@google.com", null, null, "C_AID_1", null, null,
                                null }) }, //
                { new ContactMatchTestCase( //
                        new String[][] { { null, "j.reese@google.com", "Linus Torvalds", "111-111-1111", "C_AID_1",
                                "Google", "USA", "CA" } }, //
                        new String[] { null, "j.reese@google.com", "John Reese", "999-999-9999", "C_AID_1",
                                "Facebook", "USA", "CA" }) }, //

                // Contact: Email + AID; Account: Email (Name/Location existed
                // -- Not Email only)
                { new ContactMatchTestCase( //
                        new String[][] { { null, "j.reese@google.com", "Linus Torvalds", "111-111-1111", null, null,
                                "USA", null } }, //
                        new String[] { null, "j.reese@google.com", "John Reese", "999-999-9999", null, null, "USA",
                                null }) }, //
                { new ContactMatchTestCase( //
                        new String[][] { { null, "j.reese@google.com", null, null, null, "Google", "USA", "CA" } }, //
                        new String[] { null, "j.reese@google.com", "John Reese", "999-999-9999", null, "Google", "USA",
                                "CA" }) }, //

                // Contact: Name + PhoneNumber + AID; Account: CAID
                { new ContactMatchTestCase( //
                        new String[][] { { null, null, "John Reese", "999-999-9999", "C_AID_1", null, null, null } }, //
                        new String[] { null, null, "John Reese", "(999) 999-9999", "C_AID_1", "Google", "USA",
                                "CA" }) }, //
                // One contact could have multiple email
                { new ContactMatchTestCase( //
                        new String[][] { { null, "l.torvalds@google.com", "John Reese", "999-999-9999", "C_AID_1",
                                "Google", "USA", "CA" } }, //
                        new String[] { null, "j.reese@google.com", "John Reese", "999-999-9999", "C_AID_1",
                                "Google", "USA", "CA" }) }, //

                // Contact: Name + PhoneNumber + AID; Account: Name + Location
                { new ContactMatchTestCase( //
                        new String[][] { { null, null, "John Reese", "999 999 9999", null, "Google", "USA", null } }, //
                        new String[] { null, null, "John Reese", "999-999-9999", null, "Google", "USA", "CA" }) }, //
        };
    }

    @Test(groups = "functional", dataProvider = "existedAccountNewContact", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testMatchExistedAccountNewContact(ContactMatchTestCase testCase) {
        testCase.setAccountStatus(EntityMatchStatus.MERGE_EXISTING);
        testCase.setContactStatus(EntityMatchStatus.ALLOCATE_NEW);
        matchAndVerify(testCase);
    }

    // Not cover any case related to anonymous, public domain & multi-domain
    // Schema: CustomerContactId, Email, ContactName, PhoneNumber,
    // CustomerAccountId, CompanyName, Country, State
    @DataProvider(name = "existedAccountNewContact", parallel = true)
    private Object[][] existedAccountNewContactTestData() {
        return new Object[][] { //
                // Contact: CCID; Account: CAID (Won't cover all Account match
                // key cases as matched AccountId doesn't take effect in Contact
                // match if CustomerContactId is provided)
                { new ContactMatchTestCase( //
                        new String[][] { { "C_CID_1", null, null, null, "C_AID_1", null, null, null } }, //
                        new String[] { "C_CID_2", null, null, null, "C_AID_1", null, null, null }) }, //
                { new ContactMatchTestCase( //
                        new String[][] { { "C_CID_1", null, null, null, "C_AID_1", null, null, null } }, //
                        new String[] { "C_CID_2", "j.reese@google.com", "John Reese", "999-999-9999", "C_AID_1",
                                "Google", "USA", "CA" }) }, //
                { new ContactMatchTestCase( //
                        new String[][] { { "C_CID_1", "j.reese@google.com", "John Reese", "999-999-9999", "C_AID_1",
                                "Google", "USA", "CA" } }, //
                        new String[] { "C_CID_2", null, null, null, "C_AID_1", null, null, null }) }, //
                { new ContactMatchTestCase( //
                        new String[][] { { "C_CID_1", "j.reese@google.com", "John Reese", "999-999-9999", "C_AID_1",
                                "Google", "USA", "CA" } }, //
                        new String[] { "C_CID_2", "j.reese@google.com", "John Reese", "999-999-9999", "C_AID_1",
                                "Facebook", "USA", "CA" }) }, //

                // Contact: Email + AID; Account: CAID
                { new ContactMatchTestCase( //
                        new String[][] { { null, "l.torvalds@google.com", null, null, "C_AID_1", null, null, null } }, //
                        new String[] { null, "j.reese@google.com", "John Reese", "999-999-9999", "C_AID_1", null, null,
                                null }) }, //
                { new ContactMatchTestCase( //
                        new String[][] { { null, "l.torvalds@google.com", null, null, "C_AID_1",
                                "Google", "USA", "CA" } }, //
                        new String[] { null, "j.reese@google.com", null, null, "C_AID_1",
                                "Google", "USA", "CA" }) }, //
                { new ContactMatchTestCase( //
                        new String[][] { //
                                { null, "j.reese@google.com", null, null, "C_AID_1", null, null, null }, //
                                { null, "l.torvalds@google.com", null, null, "C_AID_2", null, null, null } }, //
                        new String[] { null, "j.reese@google.com", null, null, "C_AID_2", null, null,
                                null }) }, //
                { new ContactMatchTestCase( //
                        new String[][] { //
                                { null, "j.reese@google.com", "John Reese", "999-999-9999", "C_AID_1", "Facebook",
                                        "USA", "CA" }, //
                                { null, "l.torvalds@google.com", null, null, "C_AID_2", "Facebook", "USA", "CA" } }, //
                        new String[] { null, "j.reese@google.com", "John Reese", "999-999-9999", "C_AID_2", "Facebook",
                                "USA", "CA" }) }, //

                // Contact: Email + AID; Account: Email (Name/Location existed
                // -- Not Email only)
                { new ContactMatchTestCase( //
                        new String[][] { { null, "l.torvalds@google.com", null, null, null, null,
                                "USA", null } }, //
                        new String[] { null, "j.reese@google.com", null, null, null, null, "USA",
                                null }) }, //
                { new ContactMatchTestCase( //
                        new String[][] { { null, "l.torvalds@google.com", null, null, null, "Google", "USA", "CA" } }, //
                        new String[] { null, "j.reese@google.com", "John Reese", "999-999-9999", null, "Google", "USA",
                                "CA" }) }, //
                { new ContactMatchTestCase( //
                        new String[][] { { null, "l.torvalds@google.com", "Linus Torvalds", "111-111-1111", null,
                                "Google", "USA", "CA" } }, //
                        new String[] { null, "j.reese@google.com", "John Reese", "999-999-9999", null, "Google", "USA",
                                "CA" }) }, //

                // Contact: Name + PhoneNumber + AID; Account: CAID
                { new ContactMatchTestCase( //
                        new String[][] {
                                { null, null, "John Reese", "111-111-1111", "C_AID_1", "Google", "USA", "CA" } }, //
                        new String[] { null, null, "John Reese", "(999) 999-9999", "C_AID_1", "Google", "USA",
                                "CA" }) }, //
                { new ContactMatchTestCase( //
                        new String[][] {
                                { null, null, "Linus Torvalds", "999-999-9999", "C_AID_1", "Google", "USA", "CA" } }, //
                        new String[] { null, null, "John Reese", "(999) 999-9999", "C_AID_1", "Google", "USA",
                                "CA" }) }, //
                { new ContactMatchTestCase( //
                        new String[][] {
                                { null, null, "Linus Torvalds", "111-111-1111", "C_AID_1", null, null, null } }, //
                        new String[] { null, null, "John Reese", "(999) 999-9999", "C_AID_1", null, null, null }) }, //

                // Contact: Name + PhoneNumber + AID; Account: Name + Location
                { new ContactMatchTestCase( //
                        new String[][] { { null, null, "John Reese", "111-111-1111", null, "Google", "USA", "CA" } }, //
                        new String[] { null, null, "John Reese", "(999) 999-9999", null, "Google", "USA", "CA" }) }, //
                { new ContactMatchTestCase( //
                        new String[][] {
                                { null, null, "Linus Torvalds", "999-999-9999", null, "Google", "USA", "CA" } }, //
                        new String[] { null, null, "John Reese", "(999) 999-9999", null, "Google", "USA", "CA" }) }, //
                { new ContactMatchTestCase( //
                        new String[][] {
                                { null, null, "Linus Torvalds", "111-111-1111", null, "Google", "USA", null } }, //
                        new String[] { null, null, "John Reese", "(999) 999-9999", null, "Google", "USA", null }) }, //
        };
    };

    @Test(groups = "functional", dataProvider = "newAccountExistedContact", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testMatchNewAccountExistedContact(ContactMatchTestCase testCase) {
        testCase.setAccountStatus(EntityMatchStatus.ALLOCATE_NEW);
        testCase.setContactStatus(EntityMatchStatus.MERGE_EXISTING);
        matchAndVerify(testCase);
    }

    // Not cover any case related to anonymous, public domain & multi-domain
    // Schema: CustomerContactId, Email, ContactName, PhoneNumber,
    // CustomerAccountId, CompanyName, Country, State
    @DataProvider(name = "newAccountExistedContact", parallel = true)
    private Object[][] newAccountExistedContactTestData() {
        return new Object[][] { //
                // Contact: CCID; Account: CAID (Won't cover all Account match
                // key cases as matched AccountId doesn't take effect in Contact
                // match if CustomerContactId is provided)
                { new ContactMatchTestCase( //
                        new String[][] { { "C_CID_1", null, null, null, "C_AID_1", null, null, null } }, //
                        new String[] { "C_CID_1", null, null, null, "C_AID_2", null, null, null }) }, //
                { new ContactMatchTestCase( //
                        new String[][] { { "C_CID_1", null, null, null, "C_AID_1", null, null, null } }, //
                        new String[] { "C_CID_1", "j.reese@google.com", "John Reese", "999-999-9999", "C_AID_2",
                                "Google", "USA", "CA" }) }, //
        };
    }

    @Test(groups = "functional", dataProvider = "newAccountContact", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testMatchNewAccountContact(ContactMatchTestCase testCase) {
        testCase.setAccountStatus(EntityMatchStatus.ALLOCATE_NEW);
        testCase.setContactStatus(EntityMatchStatus.ALLOCATE_NEW);
        matchAndVerify(testCase);
    }

    // Not cover any case related to anonymous, public domain & multi-domain
    // Schema: CustomerContactId, Email, ContactName, PhoneNumber,
    // CustomerAccountId, CompanyName, Country, State
    @DataProvider(name = "newAccountContact", parallel = true)
    private Object[][] newAccountContactTestData() {
        return new Object[][] { //
                // Contact: CCID; Account: CAID (Won't cover all Account match
                // key cases as matched AccountId doesn't take effect in Contact
                // match if CustomerContactId is provided)
                { new ContactMatchTestCase( //
                        new String[][] { { "C_CID_1", null, null, null, "C_AID_1", null, null, null } }, //
                        new String[] { "C_CID_2", null, null, null, "C_AID_2", null, null, null }) }, //

                // Contact: Email + AID; Account: CAID
                { new ContactMatchTestCase( //
                        new String[][] { { null, "l.torvalds@google.com", null, null, "C_AID_1", null, null, null } }, //
                        new String[] { null, "j.reese@google.com", "John Reese", "999-999-9999", "C_AID_2", null, null,
                                null }) }, //
                { new ContactMatchTestCase( //
                        new String[][] { { null, "j.reese@google.com", null, null, "C_AID_1", null, null, null } }, //
                        new String[] { null, "j.reese@google.com", null, null, "C_AID_2", null, null, null }) }, //
                { new ContactMatchTestCase( //
                        new String[][] { { null, "j.reese@google.com", null, null, "C_AID_1", null, null, null } }, //
                        new String[] { null, "j.reese@google.com", "John Reese", "999-999-9999", "C_AID_2", null, null,
                                null }) }, //
                { new ContactMatchTestCase( //
                        new String[][] { { null, "j.reese@google.com", "John Reese", "999-999-9999", "C_AID_1", null,
                                null, null } }, //
                        new String[] { null, "j.reese@google.com", "John Reese", "999-999-9999", "C_AID_2", null, null,
                                null }) }, //

                // Contact: Email + AID; Account: Email (Name/Location existed
                // -- Not Email only)

                // TODO domain/country has no match for google.com+China in 2.0.18 (have match
                // on 2.0.17), so these two matched to the same account using domain only and
                // this test does not work
                // { new ContactMatchTestCase( //
                // new String[][] { { null, "l.torvalds@google.com", null, null, null, null,
                // "China", null } }, //
                // new String[] { null, "j.reese@google.com", null, null, null, null, "USA",
                // null }) }, //
                { new ContactMatchTestCase( //
                        new String[][] { { null, "l.torvalds@facebook.com", "John Reese", "999-999-9999", null, null,
                                "USA", null } }, //
                        new String[] { null, "j.reese@google.com", "John Reese", "999-999-9999", null, null, "USA",
                                null }) }, //

                // Contact: Name + PhoneNumber + AID; Account: CAID
                { new ContactMatchTestCase( //
                        new String[][] {
                                { null, null, "John Reese", "111-111-1111", "C_AID_1", "Google", "USA", "CA" } }, //
                        new String[] { null, null, "John Reese", "(999) 999-9999", "C_AID_2", "Google", "USA",
                                "CA" }) }, //
                { new ContactMatchTestCase( //
                        new String[][] {
                                { null, null, "Linus Torvalds", "999-999-9999", "C_AID_1", "Google", "USA", "CA" } }, //
                        new String[] { null, null, "John Reese", "(999) 999-9999", "C_AID_2", "Google", "USA",
                                "CA" }) }, //
                { new ContactMatchTestCase( //
                        new String[][] {
                                { null, null, "Linus Torvalds", "111-111-1111", "C_AID_1", null, null, null } }, //
                        new String[] { null, null, "John Reese", "(999) 999-9999", "C_AID_2", null, null, null }) }, //

                // Contact: Name + PhoneNumber + AID; Account: Name + Location
                { new ContactMatchTestCase( //
                        new String[][] { { null, null, "John Reese", "111-111-1111", null, "Google", "USA", "CA" } }, //
                        new String[] { null, null, "John Reese", "(999) 999-9999", null, "Facebook", "USA", "CA" }) }, //
                { new ContactMatchTestCase( //
                        new String[][] {
                                { null, null, "Linus Torvalds", "999-999-9999", null, "Google", "USA", "CA" } }, //
                        new String[] { null, null, "John Reese", "(999) 999-9999", null, "Google", "China", null }) }, //
                { new ContactMatchTestCase( //
                        new String[][] {
                                { null, null, "Linus Torvalds", "111-111-1111", null, "Google", null, null } }, //
                        new String[] { null, null, "John Reese", "(999) 999-9999", null, "Facebook", null, null }) }, //
        };
    };

    @Test(groups = "functional", dataProvider = "validAccountAnonymousContact", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testMatchValidAccountAnonymousContact(ContactMatchTestCase testCase) {
        matchAndVerify(testCase);
    }

    // Anonymous Contact could be existing Contact or import Contact
    // Schema: CustomerContactId, Email, ContactName, PhoneNumber,
    // CustomerAccountId, CompanyName, Country, State
    @DataProvider(name = "validAccountAnonymousContact", parallel = true)
    private Object[][] validAccountAnonymousContactTestData() {
        return new Object[][] { //
                // No existed Contact, import Contact has no/incomplete match
                // key
                { new ContactMatchTestCase( //
                        null, //
                        new String[] { null, null, null, null, "C_AID_1", null, null, null }, //
                        EntityMatchStatus.ALLOCATE_NEW, EntityMatchStatus.ANONYMOUS) }, //
                { new ContactMatchTestCase( //
                        null, //
                        new String[] { null, null, "John Reese", null, null, "Google", "USA", "CA" }, //
                        EntityMatchStatus.ALLOCATE_NEW, EntityMatchStatus.ANONYMOUS) }, //
                { new ContactMatchTestCase( //
                        null, //
                        new String[] { null, null, null, "(999) 999-9999", "C_AID_1", "Google", "USA", "CA" }, //
                        EntityMatchStatus.ALLOCATE_NEW, EntityMatchStatus.ANONYMOUS) }, //

                // With existed Contact (might not saved if it's anonymous),
                // import Contact has no/incomplete match key
                { new ContactMatchTestCase( //
                        new String[][] { { null, null, null, null, "C_AID_1", null, null, null } }, //
                        new String[] { null, null, null, null, "C_AID_1", null, null, null }, //
                        EntityMatchStatus.MERGE_EXISTING, EntityMatchStatus.ANONYMOUS) }, //
                { new ContactMatchTestCase( //
                        new String[][] { { null, null, null, null, null, "Google", "USA", "CA" } }, //
                        new String[] { null, null, "John Reese", null, "C_AID_1", null, null, null }, //
                        EntityMatchStatus.ALLOCATE_NEW, EntityMatchStatus.ANONYMOUS) }, //
                { new ContactMatchTestCase( //
                        new String[][] { { null, null, "John Reese", null, null, "Google", "USA", "CA" } }, //
                        new String[] { null, null, "John Reese", null, "C_AID_1", null, null, null }, //
                        EntityMatchStatus.ALLOCATE_NEW, EntityMatchStatus.ANONYMOUS) }, //
                { new ContactMatchTestCase( //
                        new String[][] { { null, null, "John Reese", "(999) 999-9999", null, "Google", "USA", "CA" } }, //
                        new String[] { null, null, "John Reese", null, "C_AID_1", null, null, null }, //
                        EntityMatchStatus.ALLOCATE_NEW, EntityMatchStatus.ANONYMOUS) }, //
                { new ContactMatchTestCase( //
                        new String[][] { { null, null, null, null, "C_AID_1", "Google", "USA", "CA" } }, //
                        new String[] { null, null, null, "(999) 999-9999", "C_AID_1", null, null, null }, //
                        EntityMatchStatus.MERGE_EXISTING, EntityMatchStatus.ANONYMOUS) }, //
                { new ContactMatchTestCase( //
                        new String[][] { { null, null, "John Reese", null, "C_AID_1", "Google", "USA", "CA" } }, //
                        new String[] { null, null, null, "(999) 999-9999", "C_AID_1", null, null, null }, //
                        EntityMatchStatus.MERGE_EXISTING, EntityMatchStatus.ANONYMOUS) }, //
                { new ContactMatchTestCase( //
                        new String[][] { { null, null, null, "(999) 999-9999", "C_AID_1", "Google", "USA", "CA" } }, //
                        new String[] { null, null, null, "(999) 999-9999", "C_AID_1", null, null, null }, //
                        EntityMatchStatus.MERGE_EXISTING, EntityMatchStatus.ANONYMOUS) }, //
                { new ContactMatchTestCase( //
                        new String[][] {
                                { null, null, "John Reese", "(999) 999-9999", "C_AID_1", "Google", "USA", "CA" } }, //
                        new String[] { null, null, null, "(999) 999-9999", "C_AID_1", null, null, null }, //
                        EntityMatchStatus.MERGE_EXISTING, EntityMatchStatus.ANONYMOUS) }, //

                // With existed anonymous Contact (not saved), import Contact
                // has valid match key
                { new ContactMatchTestCase( //
                        new String[][] { { null, null, null, null, "C_AID_1", null, null, null } }, //
                        new String[] { null, "j.reese@google.com", null, null, "C_AID_1", null, null, null }, //
                        EntityMatchStatus.MERGE_EXISTING, EntityMatchStatus.ALLOCATE_NEW) }, //
                { new ContactMatchTestCase( //
                        new String[][] { { null, null, null, null, "C_AID_1", null, null, null } }, //
                        new String[] { null, null, "John Reese", "(999) 999-9999", "C_AID_1", null, null, null }, //
                        EntityMatchStatus.MERGE_EXISTING, EntityMatchStatus.ALLOCATE_NEW) }, //
                { new ContactMatchTestCase( //
                        new String[][] { { null, null, "John Reese", null, "C_AID_1", null, null, null } }, //
                        new String[] { null, null, "John Reese", "(999) 999-9999", "C_AID_1", null, null, null }, //
                        EntityMatchStatus.MERGE_EXISTING, EntityMatchStatus.ALLOCATE_NEW) }, //
                { new ContactMatchTestCase( //
                        new String[][] { { null, null, null, "(999) 999-9999", "C_AID_1", null, null, null } }, //
                        new String[] { null, null, "John Reese", "(999) 999-9999", "C_AID_1", null, null, null }, //
                        EntityMatchStatus.MERGE_EXISTING, EntityMatchStatus.ALLOCATE_NEW) }, //
        };
    };

    @Test(groups = "functional", dataProvider = "anonymousAccountValidContact", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testMatchAnonymousAccountValidContact(ContactMatchTestCase testCase) {
        matchAndVerify(testCase);
    }

    // Anonymous Account could for existing Contact or import Contact
    // Schema: CustomerContactId, Email, ContactName, PhoneNumber,
    // CustomerAccountId, CompanyName, Country, State
    @DataProvider(name = "anonymousAccountValidContact", parallel = true)
    private Object[][] anonymousAccountValidContactTestData() {
        return new Object[][] { //
                // Contact: CCID; Import or Existed Account: Anonymous (No valid
                // match key)
                { new ContactMatchTestCase( //
                        null, //
                        new String[] { "C_CID_2", null, null, null, null, null, null, null }, //
                        EntityMatchStatus.ANONYMOUS, EntityMatchStatus.ALLOCATE_NEW) }, //
                { new ContactMatchTestCase( //
                        new String[][] { { "C_CID_1", null, null, null, null, null, null, null } }, //
                        new String[] { "C_CID_1", null, null, null, null, null, "USA", "CA" }, //
                        EntityMatchStatus.ANONYMOUS, EntityMatchStatus.MERGE_EXISTING) }, //
                { new ContactMatchTestCase( //
                        new String[][] { { "C_CID_1", null, null, null, null, null, "USA", "CA" } }, //
                        new String[] { "C_CID_1", null, null, null, null, null, "USA", "CA" }, //
                        EntityMatchStatus.ANONYMOUS, EntityMatchStatus.MERGE_EXISTING) }, //
                { new ContactMatchTestCase( //
                        new String[][] { { "C_CID_1", null, null, null, null, "Google", "USA", "CA" } }, //
                        new String[] { "C_CID_1", null, null, null, null, null, "USA", "CA" }, //
                        EntityMatchStatus.ANONYMOUS, EntityMatchStatus.MERGE_EXISTING) }, //
                { new ContactMatchTestCase( //
                        new String[][] { { "C_CID_1", null, null, null, null, null, "USA", "CA" } }, //
                        new String[] { "C_CID_1", null, null, null, null, "Google", "USA", "CA" }, //
                        EntityMatchStatus.ALLOCATE_NEW, EntityMatchStatus.MERGE_EXISTING) }, //

                // Contact: Name + PhoneNumber; Import or Existed Account:
                // Anonymous (No valid match key)
                { new ContactMatchTestCase( //
                        null, //
                        new String[] { null, null, "John Reese", "(999) 999-9999", null, null, null, null },
                        EntityMatchStatus.ANONYMOUS, EntityMatchStatus.ALLOCATE_NEW) }, //
                { new ContactMatchTestCase( //
                        new String[][] {
                                { null, null, "John Reese", "111-111-1111", null, null, null, null } }, //
                        new String[] { null, null, "John Reese", "111-111-1111", null, null, "USA",
                                "CA" },
                        EntityMatchStatus.ANONYMOUS, EntityMatchStatus.MERGE_EXISTING) }, //
                { new ContactMatchTestCase( //
                        new String[][] { { null, null, "John Reese", "111-111-1111", null, null, "USA", "CA" } }, //
                        new String[] { null, null, "John Reese", "111-111-1111", null, null, "USA", "CA" },
                        EntityMatchStatus.ANONYMOUS, EntityMatchStatus.MERGE_EXISTING) }, //
                { new ContactMatchTestCase( //
                        new String[][] { { null, null, "John Reese", "111-111-1111", null, "Google", "USA", "CA" } }, //
                        new String[] { null, null, "John Reese", "111-111-1111", null, null, "USA", "CA" },
                        EntityMatchStatus.ANONYMOUS, EntityMatchStatus.MERGE_EXISTING) }, //
                // TODO anonymous account + NP won't be able to create AID+NP entry, so this
                // case does not work
                // { new ContactMatchTestCase( //
                // new String[][] { { null, null, "John Reese", "111-111-1111", null, null,
                // "USA", "CA" } }, //
                // new String[] { null, null, "John Reese", "111-111-1111", null, "Google",
                // "USA", "CA" },
                // EntityMatchStatus.ALLOCATE_NEW, EntityMatchStatus.MERGE_EXISTING) }, //

                // Import Contact: Email; Import Account: Email only (AID is
                // returned in match result, but in Contact lookup, matched AID
                // is treated as anonymous)
                { new ContactMatchTestCase( //
                        null, //
                        new String[] { null, "j.reese@google.com", null, null, null, null, null, null }, //
                        EntityMatchStatus.ALLOCATE_NEW, EntityMatchStatus.ALLOCATE_NEW) }, //
                { new ContactMatchTestCase( //
                        new String[][] { { null, "j.reese@google.com", null, null, null, null, "USA", null } }, //
                        new String[] { null, "j.reese@google.com", null, null, null, null, null, null }, //
                        EntityMatchStatus.MERGE_EXISTING, EntityMatchStatus.MERGE_EXISTING) }, //
                { new ContactMatchTestCase( //
                        new String[][] { { null, "j.reese@google.com", null, null, null, null, null, null } }, //
                        new String[] { null, "j.reese@google.com", null, null, null, null, "USA", null }, //
                        EntityMatchStatus.MERGE_EXISTING, EntityMatchStatus.MERGE_EXISTING) }, //
                { new ContactMatchTestCase( //
                        new String[][] { { null, "j.reese@google.com", null, null, "C_AID_1", null, null, null } }, //
                        new String[] { null, "j.reese@google.com", null, null, null, null, null, null }, //
                        EntityMatchStatus.MERGE_EXISTING, EntityMatchStatus.MERGE_EXISTING) }, //
                { new ContactMatchTestCase( //
                        new String[][] { { null, "j.reese@google.com", null, null, null, null, null, null } }, //
                        new String[] { null, "j.reese@google.com", null, null, "C_AID_1", null, null, null }, //
                        EntityMatchStatus.MERGE_EXISTING, EntityMatchStatus.MERGE_EXISTING) }, //
                { new ContactMatchTestCase( //
                        new String[][] { //
                                { null, "l.torvalds@google.com", null, null, "C_AID_1", null, null, null }, //
                                { null, "j.reese@google.com", null, null, "C_AID_2", null, null, null } }, //
                        new String[] { null, "j.reese@google.com", null, null, null, null, null, null }, //
                        // merge to account 2 (use email to match to account 1 but use the account in
                        // contact 2 since it is email only) and contact 2 (matched with Email only)
                        EntityMatchStatus.MERGE_EXISTING, EntityMatchStatus.MERGE_EXISTING, 1, 1) }, //
        };
    }

    @Test(groups = "functional", dataProvider = "anonymousAccountContact", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testMatchAnonymousAccountContact(ContactMatchTestCase testCase) {
        matchAndVerify(testCase);
    }

    // Anonymous Account/Contact could for existing Contact or import Contact
    // (Not cover public domain)
    // Schema: CustomerContactId, Email, ContactName, PhoneNumber,
    // CustomerAccountId, CompanyName, Country, State
    // FIXME: Somehow only for this case, parallel = true keeps hitting
    // exception org.testng.TestNGException:
    // java.util.concurrent.ExecutionException:
    // java.lang.ArrayIndexOutOfBoundsException: -1
    // Will revisit later
    @DataProvider(name = "anonymousAccountContact", parallel = true)
    private Object[][] anonymousAccountContactTestData() {
        return new Object[][] { //
                { new ContactMatchTestCase( //
                        null, //
                        new String[] { null, null, null, null, null, null, null, null }, //
                        EntityMatchStatus.ANONYMOUS, EntityMatchStatus.ANONYMOUS) }, //
                { new ContactMatchTestCase( //
                        null, //
                        new String[] { null, null, "John Reese", null, null, null, "USA", null }, //
                        EntityMatchStatus.ANONYMOUS, EntityMatchStatus.ANONYMOUS) }, //
                { new ContactMatchTestCase( //
                        new String[][] { { null, null, "John Reese", "111-111-1111", null, null, "USA", null } }, //
                        new String[] { null, null, "John Reese", null, null, null, "USA", null }, //
                        EntityMatchStatus.ANONYMOUS, EntityMatchStatus.ANONYMOUS) }, //
                { new ContactMatchTestCase( //
                        null, //
                        new String[] { null, null, null, "111-111-1111", null, null, "USA", "CA" }, //
                        EntityMatchStatus.ANONYMOUS, EntityMatchStatus.ANONYMOUS) }, //
                { new ContactMatchTestCase( //
                        new String[][] { { null, null, "John Reese", "111-111-1111", null, null, "USA", "CA" } }, //
                        new String[] { null, null, null, "111-111-1111", null, null, "USA", "CA" }, //
                        EntityMatchStatus.ANONYMOUS, EntityMatchStatus.ANONYMOUS) }, //
                { new ContactMatchTestCase( //
                        new String[][] { { null, null, "John Reese", null, null, null, "USA", "CA" } }, //
                        new String[] { null, null, "John Reese", "111-111-1111", null, "Google", "USA", "CA" }, //
                        EntityMatchStatus.ALLOCATE_NEW, EntityMatchStatus.ALLOCATE_NEW) }, //
                { new ContactMatchTestCase( //
                        new String[][] { { null, null, null, "111-111-1111", null, null, "USA", null } }, //
                        new String[] { null, null, "John Reese", "111-111-1111", null, "Google", "USA", "CA" }, //
                        EntityMatchStatus.ALLOCATE_NEW, EntityMatchStatus.ALLOCATE_NEW) }, //
        };
    }

    @Test(groups = "functional", dataProvider = "emailOnly", retryAnalyzer = SimpleRetryAnalyzer.class)
    private void testMatchEmailOnlyContacts(ContactMatchTestCase testCase) {
        matchAndVerify(testCase);
    }

    @DataProvider(name = "emailOnly", parallel = true)
    private Object[][] emailOnlyContactsTestData() {
        return new Object[][] { //
                { new ContactMatchTestCase( //
                        null, //
                        new String[] { "C_CID_1", "j.reese@google.com", null, null, null, null, null, null }, //
                        EntityMatchStatus.ALLOCATE_NEW, EntityMatchStatus.ALLOCATE_NEW) }, //
                // existing data is email only (not saving AID), so result is netflix's AID
                { new ContactMatchTestCase( //
                        new String[][] { //
                                { "C_CID_1", "j.reese@google.com", null, null, null, null, null, null }, //
                        }, //
                        new String[] { "C_CID_1", "j.reese@netflix.com", null, null, null, null, null, null }, //
                        EntityMatchStatus.ALLOCATE_NEW, EntityMatchStatus.MERGE_EXISTING) }, //
                // existing data has account info (save AID), import data match to a different
                // AID but will use AID from existing Contact because import is email only
                { new ContactMatchTestCase( //
                        new String[][] { //
                                { "C_CID_1", "j.reese@google.com", null, null, "C_AID_1", null, null, null },
                        //
                        }, //
                        new String[] { "C_CID_1", "j.reese@netflix.com", null, null, null, null, null, null }, //
                        EntityMatchStatus.MERGE_EXISTING, EntityMatchStatus.MERGE_EXISTING) }, //
                // import data is NOT email only, make sure no regression
                { new ContactMatchTestCase( //
                        new String[][] { //
                                { "C_CID_1", "j.reese@google.com", null, null, null, null, null, null }, //
                        }, //
                        new String[] { "C_CID_1", "j.reese@netflix.com", null, "C_AID_2", null, null, null, null }, //
                        EntityMatchStatus.ALLOCATE_NEW, EntityMatchStatus.MERGE_EXISTING) }, //
                { new ContactMatchTestCase( //
                        new String[][] { //
                                { "C_CID_1", "j.reese@google.com", null, null, "C_AID_1", null, null, null }, //
                        }, //
                        new String[] { "C_CID_1", "j.reese@netflix.com", null, null, "C_AID_2", null, null, null }, //
                        EntityMatchStatus.ALLOCATE_NEW, EntityMatchStatus.MERGE_EXISTING) }, //
                // NOTE with current implementation, any account field counts as not email only,
                // even if there is no valid account match key combination (state only in this
                // case)
                { new ContactMatchTestCase( //
                        new String[][] { //
                                { "C_CID_1", "j.reese@google.com", null, null, null, null, null, "CA" }, //
                        }, //
                        new String[] { "C_CID_1", "j.reese@netflix.com", null, null, null, null, null, null }, //
                        EntityMatchStatus.MERGE_EXISTING, EntityMatchStatus.MERGE_EXISTING) }, //
        };
    }

    private void matchAndVerify(ContactMatchTestCase testCase) {
        Tenant tenant = newTestTenant();
        testCase.setTenant(tenant);

        List<String> contactEntityIds = new ArrayList<>();
        List<String> accountEntityIds = new ArrayList<>();
        if (testCase.existingData != null) {
            for (String[] existingData : testCase.existingData) {
                // populate existing data
                Pair<MatchInput, MatchOutput> result = matchContactWithDefaultFields(tenant, existingData);
                MatchOutput output = result.getRight();
                Assert.assertNotNull(output,
                        String.format("MatchOutput of existing data for test case %s should not be null", testCase));
                // if case status = MERGE_EXISTING, put merge target as last
                // record in existingData. Could improve in future if merge
                // targets of Account & Contact need to be in different records
                contactEntityIds.add(verifyAndGetEntityId(output, InterfaceName.ContactId.name()));
                accountEntityIds.add(getColumnValue(output, AccountId.name()));
            }
        }

        // match import data
        Pair<MatchInput, MatchOutput> result = matchContactWithDefaultFields(tenant, testCase.importData);
        MatchOutput output = result.getRight();
        Assert.assertNotNull(output,
                String.format("MatchOutput of import data for test case %s should not be null", testCase));
        String importContactEntityId = verifyAndGetEntityId(output, InterfaceName.ContactId.name());
        String importAccountEntityId = getColumnValue(output, AccountId.name());

        verifyEntityId(BusinessEntity.Contact.name(), importContactEntityId, contactEntityIds, testCase.contactStatus,
                testCase.mergedContactIdx, testCase);
        verifyEntityId(BusinessEntity.Account.name(), importAccountEntityId, accountEntityIds, testCase.accountStatus,
                testCase.mergedAccountIdx, testCase);
    }

    private void verifyEntityId(String entity, String importEntityId, List<String> existingEntityIds,
            EntityMatchStatus status, int mergedEntityIdx,
            ContactMatchTestCase testCase) {
        if (status == EntityMatchStatus.ALLOCATE_NEW) {
            Assert.assertFalse(existingEntityIds.contains(importEntityId),
                    String.format(
                            "%sEntityId for import data (%s) should not be the same as any existing data (%s) for test case %s",
                            entity, importEntityId, existingEntityIds, testCase));
        } else if (status == EntityMatchStatus.MERGE_EXISTING) {
            if (mergedEntityIdx < 0) {
                // start from the end
                mergedEntityIdx = existingEntityIds.size() + mergedEntityIdx;
            }
            Assert.assertEquals(importEntityId, existingEntityIds.get(mergedEntityIdx),
                    String.format(
                            "%sEntityId for import data (%s) should be the same as existing data at idx=%d for test case %s",
                            entity, importEntityId, mergedEntityIdx, testCase));
        } else if (status == EntityMatchStatus.ANONYMOUS) {
            Assert.assertEquals(importEntityId, DataCloudConstants.ENTITY_ANONYMOUS_ID,
                    String.format("%sEntityId for import data (%s) should be anonymous for test case %s", entity,
                            importEntityId, testCase));
        }
    }

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
        // if case status = MERGE_EXISTING, put merge target as last record in
        // existingData.
        String[][] existingData;
        String[] importData;
        EntityMatchStatus accountStatus;
        EntityMatchStatus contactStatus;
        // negative means starting from the end of the list (e.g., -1 is the last
        // record)
        int mergedAccountIdx = -1;
        int mergedContactIdx = -1;
        Tenant tenant;

        ContactMatchTestCase(String[][] existingData, String[] importData) {
            this.fields = DEFAULT_FIELDS;
            this.existingData = existingData;
            this.importData = importData;
        }

        ContactMatchTestCase(String[][] existingData, String[] importData, EntityMatchStatus accountStatus,
                EntityMatchStatus contactStatus) {
            this.fields = DEFAULT_FIELDS;
            this.existingData = existingData;
            this.importData = importData;
            this.accountStatus = accountStatus;
            this.contactStatus = contactStatus;
        }

        ContactMatchTestCase(String[][] existingData, String[] importData, EntityMatchStatus accountStatus,
                EntityMatchStatus contactStatus, int mergedAccountIdx, int mergedContactIdx) {
            this(existingData, importData, accountStatus, contactStatus);
            this.mergedAccountIdx = mergedAccountIdx;
            this.mergedContactIdx = mergedContactIdx;
        }

        ContactMatchTestCase(String[] fields, String[][] existingData, String[] importData,
                EntityMatchStatus accountStatus, EntityMatchStatus contactStatus) {
            this.fields = fields;
            this.existingData = existingData;
            this.importData = importData;
            this.accountStatus = accountStatus;
            this.contactStatus = contactStatus;
        }

        public void setTenant(Tenant tenant) {
            this.tenant = tenant;
        }

        void setAccountStatus(EntityMatchStatus accountStatus) {
            this.accountStatus = accountStatus;
        }

        void setContactStatus(EntityMatchStatus contactStatus) {
            this.contactStatus = contactStatus;
        }

        @Override
        public String toString() {
            return "ContactMatchTestCase{" //
                    + "tenant=" + (tenant == null ? null : tenant.getId()) //
                    + ", testCase=" + idx //
                    + ", fields=" + Arrays.toString(fields) //
                    + ", existingData="
                    + (existingData == null
                            ? "[]"
                            : Arrays.stream(existingData).map(Arrays::toString)
                                    .collect(
                                            Collectors.joining(System.lineSeparator()))) //
                    + ", importData=" + Arrays.toString(importData) //
                    + ", accountStatus=" + accountStatus //
                    + ", contactStatus=" + contactStatus + '}';
        }
    }

    private enum EntityMatchStatus {
        ALLOCATE_NEW, // allocate a new entity
        MERGE_EXISTING, // merge to an existing entity
        ANONYMOUS // match to anonymous entity
    }
}
