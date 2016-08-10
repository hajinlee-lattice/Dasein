package com.latticeengines.propdata.match.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.propdata.match.AccountLookupEntry;
import com.latticeengines.domain.exposed.propdata.match.AccountLookupRequest;
import com.latticeengines.domain.exposed.propdata.match.LatticeAccount;
import com.latticeengines.propdata.match.entitymanager.LatticeAccountMgr;
import com.latticeengines.propdata.match.entitymanager.AccountLookupEntryMgr;
import com.latticeengines.propdata.match.service.AccountLookupService;
import com.latticeengines.propdata.match.testframework.PropDataMatchFunctionalTestNGBase;

public class AccountLookupServiceImplTestNG extends PropDataMatchFunctionalTestNGBase {

    private static final Log log = LogFactory.getLog(AccountLookupServiceImpl.class);

    public static final String version = "1.0";
    public static final int NumOfEntities = 8;
    public static final int NumOfLookups = 4;

    @Autowired
    private AccountLookupService accountLookupService;

    private LatticeAccountMgr accountMgr;
    private AccountLookupEntryMgr lookupMgr;

    private List<LatticeAccount> accounts;
    private List<AccountLookupEntry> lookups;

    @BeforeClass(groups = "functional")
    public void beforeClass() {
        accountMgr = accountLookupService.getAccountMgr(version);
        lookupMgr = accountLookupService.getLookupMgr(version);
        initEntities();
    }

    @Test(groups = "functional")
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

    @Test(groups = "functional")
    public void testLookupFailure() {

        createEntities();

        AccountLookupRequest request = createLookupRequest(true, true, false);

        List<LatticeAccount> accounts = accountLookupService.batchLookup(request);

        verifyLookupRes(accounts, false);

        deleteEntities();
    }

    private void initEntities() {

        accounts = new ArrayList<LatticeAccount>();
        lookups = new ArrayList<AccountLookupEntry>();
        for (int i = 0; i < NumOfEntities; i++) {
            String accountId = "FakeAccount" + i;
            String domain = "FakeDomain" + i;
            String duns = "FakeDuns" + i;

            LatticeAccount account = new LatticeAccount();
            account.setId(accountId);
            Map<String, String> attributes = new HashMap<String, String>();
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
                String domain = account.getAttributes().get("Domain");
                String duns = account.getAttributes().get("Duns");
                Assert.assertEquals(domain, "FakeDomain" + index);
                Assert.assertEquals(duns, "FakeDuns" + index);

            } else {
                Assert.assertEquals(account, null);
            }
        }
    }
}
