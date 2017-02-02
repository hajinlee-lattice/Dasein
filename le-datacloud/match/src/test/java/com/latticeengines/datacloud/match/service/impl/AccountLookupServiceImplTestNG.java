package com.latticeengines.datacloud.match.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.match.exposed.service.AccountLookupService;
import com.latticeengines.datacloud.match.testframework.DataCloudMatchFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupRequest;

public class AccountLookupServiceImplTestNG extends DataCloudMatchFunctionalTestNGBase {

    @Autowired
    private AccountLookupService accountLookupService;

    @Autowired
    private DataCloudVersionEntityMgr versionEntityMgr;

    @Value("${common.le.stack}")
    private String leStack;

    @Test(groups = "functional")
    public void testUpdateLookup() {
        String domain = leStack + "_testdomain";
        String duns = leStack + "_testduns";
        String accountId1 = "TestLatticeAccountId_1";
        String accountId2 = "TestLatticeAccountId_2";

        String dataCloudVersion = versionEntityMgr.currentApprovedVersion().getVersion();

        AccountLookupEntry lookupEntry = new AccountLookupEntry();
        lookupEntry.setDomain(domain);
        lookupEntry.setDuns(duns);
        lookupEntry.setPatched(true);
        lookupEntry.setLatticeAccountId(accountId1);
        accountLookupService.updateLookupEntry(lookupEntry, dataCloudVersion);

        AccountLookupRequest request = new AccountLookupRequest(dataCloudVersion);
        request.addId(lookupEntry.getId());
        String id = accountLookupService.batchLookupIds(request).get(0);
        Assert.assertEquals(id, accountId1);

        lookupEntry.setLatticeAccountId(accountId2);
        accountLookupService.updateLookupEntry(lookupEntry, dataCloudVersion);

        id = accountLookupService.batchLookupIds(request).get(0);
        Assert.assertEquals(id, accountId2);
    }

}
