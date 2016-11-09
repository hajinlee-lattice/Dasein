package com.latticeengines.datacloud.match.service.impl;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.datacloud.match.entitymgr.AccountLookupEntryMgr;
import com.latticeengines.datacloud.match.entitymgr.LatticeAccountMgr;
import com.latticeengines.datacloud.match.exposed.service.AccountLookupService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupRequest;
import com.latticeengines.domain.exposed.datacloud.match.LatticeAccount;

public class AccountLookupServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final Log log = LogFactory.getLog(AccountLookupServiceImplTestNG.class);

    public static final String version = "1.0";
    public static final int NumOfEntities = 8;
    public static final int NumOfLookups = 4;

    @Autowired
    private AccountLookupService accountLookupService;

    private LatticeAccountMgr accountMgr;
    private AccountLookupEntryMgr lookupMgr;

    private List<LatticeAccount> accounts;
    private List<AccountLookupEntry> lookups;

    @BeforeClass(groups = "functional", enabled = false)
    public void beforeClass() {
        accountMgr = accountLookupService.getAccountMgr(version);
        lookupMgr = accountLookupService.getLookupMgr(version);
        initEntities();
        log.info("Loading objects from csv file");
        loadIndexFromCsv();
        loadAccountFromCsv();
    }

    @Test(groups = "functional", enabled = false)
    public void testLookup() {

        batchCreateEntities();

        AccountLookupRequest request = createLookupRequest(true, true, true);
        List<LatticeAccount> results = accountLookupService.batchLookup(request);
        verifyLookupRes(results, true);

        request = createLookupRequest(true, false, true);
        results = accountLookupService.batchLookup(request);
        verifyLookupRes(results, true);

        request = createLookupRequest(false, true, true);
        results = accountLookupService.batchLookup(request);
        verifyLookupRes(results, true);

        deleteEntities();
    }

    @Test(groups = "functional", enabled = false)
    public void testLookupFailure() {

        createEntities();

        AccountLookupRequest request = createLookupRequest(true, true, false);

        List<LatticeAccount> accounts = accountLookupService.batchLookup(request);

        verifyLookupRes(accounts, false);

        deleteEntities();
    }

    private void initEntities() {

        accounts = new ArrayList<>();
        lookups = new ArrayList<>();
        for (int i = 0; i < NumOfEntities; i++) {
            String accountId = "FakeAccount" + i;
            String domain = "FakeDomain" + i;
            String duns = "FakeDuns" + i;

            LatticeAccount account = new LatticeAccount();
            account.setId(accountId);
            Map<String, Object> attributes = new HashMap<>();
            attributes.put("Domain", domain);
            attributes.put("Duns", duns);
            account.setAttributes(attributes);
            accounts.add(account);

            AccountLookupEntry lookupEntry = new AccountLookupEntry();
            lookupEntry.setDomain(domain);
            lookupEntry.setDuns(duns);
            lookupEntry.setLatticeAccountId(accountId);
            lookups.add(lookupEntry);

            lookupEntry = new AccountLookupEntry();
            lookupEntry.setDomain(domain);
            lookupEntry.setLatticeAccountId(accountId);
            lookups.add(lookupEntry);

            lookupEntry = new AccountLookupEntry();
            lookupEntry.setDuns(duns);
            lookupEntry.setLatticeAccountId(accountId);
            lookups.add(lookupEntry);
        }
    }

    private void loadIndexFromCsv() {

        URL url = Thread.currentThread().getContextClassLoader()
                  .getResource("com/latticeengines/propdata/match/AccountMaster.csv");
        Assert.assertNotNull(url, "Cannot find AccountMaster.csv");

        try {
            CSVParser parser = CSVParser.parse(new File(url.getFile()), Charset.forName("UTF-8"), LECSVFormat.format);
            for (CSVRecord csvRecord : parser) {
                AccountLookupEntry lookupEntry = new AccountLookupEntry();
                lookupEntry.setLatticeAccountId(csvRecord.get(0));
                lookupEntry.setDomain(csvRecord.get(1));
                lookupEntry.setDuns(csvRecord.get(2));
                lookupMgr.create(lookupEntry);
            }
        } catch (IOException e) {
            Assert.fail("Failed to load AccountMasterIndex from csv. ", e);
        }
    }

    private void loadAccountFromCsv() {

        URL url = Thread.currentThread().getContextClassLoader()
                  .getResource("com/latticeengines/propdata/match/AccountMasterSample.csv");
        Assert.assertNotNull(url, "Cannot find AccountMaster.csv");

        try {
            CSVParser parser = CSVParser.parse(new File(url.getFile()), Charset.forName("UTF-8"), LECSVFormat.format);
            for (CSVRecord csvRecord : parser) {
                Map<String, String> csvMap = csvRecord.toMap();
                Map<String, Object> attributes = new HashMap<>();
                for (Map.Entry<String, String> entry: csvMap.entrySet()) {
                    attributes.put(entry.getKey(), entry.getValue());
                }
                LatticeAccount account = new LatticeAccount();
                account.setId(csvRecord.get(0));
                account.setAttributes(attributes);
                accountMgr.create(account);
            }
        } catch (IOException e) {
            Assert.fail("Failed to load account pool from Acco", e);
        }
    }

    private void createEntities() {
        for (LatticeAccount account : accounts)
            accountMgr.create(account);

        for (AccountLookupEntry lookup : lookups)
            lookupMgr.create(lookup);

    }

    private void batchCreateEntities() {
        accountMgr.batchCreate(accounts);
        lookupMgr.batchCreate(lookups);
    }

    private void deleteEntities() {
        for (LatticeAccount account : accounts)
            accountMgr.delete(account);

        for (AccountLookupEntry lookup : lookups)
            lookupMgr.delete(lookup);

    }

    private AccountLookupRequest createLookupRequest(boolean useDomain, boolean useDuns, boolean isValid) {
        AccountLookupRequest request = new AccountLookupRequest(version);
        for (int i = 0; i < NumOfLookups; i++) {
            int index = i * (NumOfEntities / NumOfLookups) + 1;
            String domain = null;
            String duns = null;
            if (useDomain)
                domain = isValid ? "FakeDomain" + index : "RealDomain" + index;
            if (useDuns)
                duns = isValid ? "FakeDuns" + index : "RealDuns" + index;
            request.addLookupPair(domain, duns);
        }

        return request;
    }

    private void verifyLookupRes(List<LatticeAccount> accounts, boolean isMatch) {
        Assert.assertEquals(accounts.size(), NumOfLookups);
        for (int i = 0; i < NumOfLookups; i++) {
            LatticeAccount account = accounts.get(i);
            if (isMatch) {
                int index = i * (NumOfEntities / NumOfLookups) + 1;
                Assert.assertEquals(account.getId(), "FakeAccount" + index);
                String domain = (String) account.getAttributes().get("Domain");
                String duns = (String) account.getAttributes().get("Duns");
                Assert.assertEquals(domain, "FakeDomain" + index);
                Assert.assertEquals(duns, "FakeDuns" + index);

            } else {
                Assert.assertEquals(account, null);
            }
        }
    }
}
