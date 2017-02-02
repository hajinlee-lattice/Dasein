package com.latticeengines.datacloud.match.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.exposed.service.AccountLookupService;
import com.latticeengines.datacloud.match.exposed.service.PatchService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupRequest;
import com.latticeengines.domain.exposed.datacloud.match.LookupUpdateRequest;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@Component
public class PatchServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final String ALPHABET_ACCOUNT_ID = "94418525";

    @Autowired
    private PatchService patchService;

    @Autowired
    private AccountLookupService accountLookupService;

    @Value("${common.le.stack}")
    private String leStack;

    private PatchServiceImpl patchServiceImpl;

    @BeforeClass(groups = "functional")
    public void setup() {
        patchServiceImpl = (PatchServiceImpl) patchService;
    }

    @Test(groups = "functional")
    public void mustSpecifyValidLatticeAccountId() {
        LookupUpdateRequest updateRequest = new LookupUpdateRequest();
        MatchKeyTuple tuple = keyTuple("google.com", "Google Inc", "Mountain View", "CA", "USA");
        updateRequest.setMatchKeys(tuple);

        boolean exception = false;
        try {
            patchServiceImpl.verifyCanPath(updateRequest);
        } catch (LedpException e) {
            exception = true;
            Assert.assertEquals(e.getCode(), LedpCode.LEDP_25030);
        }
        Assert.assertTrue(exception);

        updateRequest.setLatticeAccountId("not exists");
        exception = false;
        try {
            patchServiceImpl.verifyCanPath(updateRequest);
        } catch (LedpException e) {
            exception = true;
            Assert.assertEquals(e.getCode(), LedpCode.LEDP_25031);
        }
        Assert.assertTrue(exception);
    }

    @Test(groups = "functional")
    public void cannotPathDomainLocationNoName() {
        LookupUpdateRequest updateRequest = new LookupUpdateRequest();
        MatchKeyTuple tuple = keyTuple("google.com", null, null, "CA", "USA");
        updateRequest.setMatchKeys(tuple);
        updateRequest.setLatticeAccountId("12345");

        boolean exception = false;
        try {
            patchServiceImpl.verifyCanPath(updateRequest);
        } catch (LedpException e) {
            exception = true;
            Assert.assertEquals(e.getCode(), LedpCode.LEDP_25033);
        }
        Assert.assertTrue(exception);
    }

    @Test(groups = "functional")
    public void patchDomainOnly() {
        String domain = leStack + "_TestDomainToPatch.com";
        String accountId1 = leStack + "_TestPatchAccountId_1";
        String accountId2 = leStack + "_TestPatchAccountId_2";

        AccountLookupEntry lookupEntry = new AccountLookupEntry();
        lookupEntry.setDomain(domain);
        lookupEntry.setPatched(true);
        lookupEntry.setLatticeAccountId(accountId1);
        accountLookupService.updateLookupEntry(lookupEntry, currentDataCloudVersion);

        AccountLookupRequest request = new AccountLookupRequest(currentDataCloudVersion);
        request.addId(lookupEntry.getId());
        String id = accountLookupService.batchLookupIds(request).get(0);
        Assert.assertEquals(id, accountId1);

        LookupUpdateRequest updateRequest = new LookupUpdateRequest();
        MatchKeyTuple tuple = keyTuple(domain, null, null, null, null);
        updateRequest.setMatchKeys(tuple);
        updateRequest.setLatticeAccountId(accountId2);

        boolean exception = false;
        try {
            patchServiceImpl.verifyCanPath(updateRequest);
        } catch (LedpException e) {
            exception = true;
            Assert.assertEquals(e.getCode(), LedpCode.LEDP_25031);
        }
        Assert.assertTrue(exception);

        updateRequest.setLatticeAccountId(ALPHABET_ACCOUNT_ID);
        patchServiceImpl.patch(updateRequest);

        request = new AccountLookupRequest(currentDataCloudVersion);
        lookupEntry = new AccountLookupEntry();
        lookupEntry.setDomain(domain);
        request.addId(lookupEntry.getId());
        id = accountLookupService.batchLookupIds(request).get(0);
        Assert.assertEquals(id, ALPHABET_ACCOUNT_ID);
    }

    private MatchKeyTuple keyTuple(String domain, String name, String city, String state, String country) {
        MatchKeyTuple tuple = new MatchKeyTuple();
        tuple.setDomain(domain);
        tuple.setName(name);
        tuple.setCity(city);
        tuple.setState(state);
        tuple.setCountry(country);
        return tuple;
    }

}
