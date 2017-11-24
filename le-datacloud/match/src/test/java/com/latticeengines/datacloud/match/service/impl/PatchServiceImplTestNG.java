package com.latticeengines.datacloud.match.service.impl;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.DomainUtils;
import com.latticeengines.datacloud.core.service.DnBCacheService;
import com.latticeengines.datacloud.match.exposed.service.AccountLookupService;
import com.latticeengines.datacloud.match.exposed.service.PatchService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBCache;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupRequest;
import com.latticeengines.domain.exposed.datacloud.match.LookupUpdateRequest;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@Component
public class PatchServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    private static final String ALPHABET_ACCOUNT_ID = "450000298086";
    private static final String GREG_GOOGLER_ACCOUNT_ID = "530000381629";
    private static final String GREG_GOOGLER_DUNS = "039024002";

    @Autowired
    private PatchService patchService;

    @Autowired
    private AccountLookupService accountLookupService;

    @Autowired
    private DnBCacheService dnBCacheService;

    @Value("${common.le.stack}")
    private String leStack;

    private PatchServiceImpl patchServiceImpl;

    @BeforeClass(groups = "functional")
    public void setup() {
        patchServiceImpl = (PatchServiceImpl) patchService;
        if (StringUtils.isEmpty(leStack)) {
            leStack = "default";
        }
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
    public void cannotPatchDomainLocationNoName() {
        LookupUpdateRequest updateRequest = new LookupUpdateRequest();
        MatchKeyTuple tuple = keyTuple("google.com", null, null, "CA", "USA");
        updateRequest.setMatchKeys(tuple);
        updateRequest.setLatticeAccountId(ALPHABET_ACCOUNT_ID);

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
        String domain = DomainUtils.parseDomain(leStack.toLowerCase() + "testdomaintopatch.com");
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

        exception = false;
        try {
            patchServiceImpl.patch(updateRequest);
        } catch (LedpException e) {
            exception = true;
            Assert.assertEquals(e.getCode(), LedpCode.LEDP_25034);
        }
        Assert.assertTrue(exception);
    }

    @Test(groups = "functional")
    public void patchPublicDomain() {
        String publicDomain = "gmail.com";

        LookupUpdateRequest updateRequest = new LookupUpdateRequest();
        MatchKeyTuple tuple = keyTuple(publicDomain, null, null, null, null);
        updateRequest.setMatchKeys(tuple);
        updateRequest.setLatticeAccountId(ALPHABET_ACCOUNT_ID);

        boolean exception = false;
        try {
            patchServiceImpl.patch(updateRequest);
        } catch (LedpException e) {
            exception = true;
            Assert.assertEquals(e.getCode(), LedpCode.LEDP_25035);
        }
        Assert.assertTrue(exception);
    }

    @Test(groups = "functional")
    public void patchNameLocation() {
        LookupUpdateRequest updateRequest = new LookupUpdateRequest();
        MatchKeyTuple tuple = keyTuple(null, "garbagename12345".toUpperCase(), leStack.toUpperCase(), "CA", "USA");
        String cacheId = "_CITY_" + leStack.toUpperCase() + "_COUNTRYCODE_US_NAME_GARBAGENAME12345_PHONE_NULL_STATE_CALIFORNIA_ZIPCODE_NULL";
        updateRequest.setMatchKeys(tuple);
        updateRequest.setLatticeAccountId(GREG_GOOGLER_ACCOUNT_ID);

        DnBCache dnBCache = new DnBCache();
        dnBCache.setId(cacheId);
        dnBCacheService.removeCache(dnBCache);

        boolean exception = false;
        try {
            patchServiceImpl.patch(updateRequest);
        } catch (LedpException e) {
            exception = true;
            Assert.assertEquals(e.getCode(), LedpCode.LEDP_25036);
        }
        Assert.assertTrue(exception);

        dnBCacheService.removeCache(dnBCache);

        tuple = keyTuple(null, "Googler Greg".toUpperCase(), leStack.toUpperCase(), "CA", "USA");
        cacheId = "_CITY_" + leStack.toUpperCase() + "_COUNTRYCODE_US_NAME_GOOGLER GREG_PHONE_NULL_STATE_CALIFORNIA_ZIPCODE_NULL";
        updateRequest.setMatchKeys(tuple);

        dnBCache.setId(cacheId);
        dnBCacheService.removeCache(dnBCache);

        patchServiceImpl.patch(updateRequest);

        DnBCache cacheInDynamo = dnBCacheService.getCacheMgr().findByKey(dnBCache.getId());
        cacheInDynamo.parseCacheContext();
        Assert.assertEquals(cacheInDynamo.getDuns(), GREG_GOOGLER_DUNS);
        Assert.assertTrue(cacheInDynamo.isWhiteCache());
        Assert.assertTrue(cacheInDynamo.getPatched());

        dnBCacheService.removeCache(dnBCache);
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
