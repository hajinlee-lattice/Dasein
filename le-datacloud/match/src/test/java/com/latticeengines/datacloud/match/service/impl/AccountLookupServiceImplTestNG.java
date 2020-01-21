package com.latticeengines.datacloud.match.service.impl;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.match.exposed.service.AccountLookupService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupRequest;
public class AccountLookupServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    @Inject
    private AccountLookupService accountLookupService;

    @Value("${common.le.stack}")
    private String leStack;

    @Test(groups = "functional")
    public void testUpdateLookup() {
        String domain = leStack + "_testdomain.com";
        String duns = leStack + "_testduns";
        String accountId1 = leStack + "_TestLatticeAccountId_1";
        String accountId2 = leStack + "_TestLatticeAccountId_2";

        AccountLookupEntry lookupEntry = new AccountLookupEntry();
        lookupEntry.setDomain(domain);
        lookupEntry.setDuns(duns);
        lookupEntry.setPatched(true);
        lookupEntry.setLatticeAccountId(accountId1);
        accountLookupService.updateLookupEntry(lookupEntry, currentDataCloudVersion);

        AccountLookupRequest request = new AccountLookupRequest(currentDataCloudVersion);
        request.addId(lookupEntry.getId());
        String id = accountLookupService.batchLookupIds(request).get(0);
        Assert.assertEquals(id, accountId1);

        lookupEntry.setLatticeAccountId(accountId2);
        accountLookupService.updateLookupEntry(lookupEntry, currentDataCloudVersion);

        id = accountLookupService.batchLookupIds(request).get(0);
        Assert.assertEquals(id, accountId2);
    }

}
