package com.latticeengines.datacloud.match.exposed.service;

import java.util.List;

import com.latticeengines.datacloud.match.entitymgr.AccountLookupEntryMgr;
import com.latticeengines.datacloud.match.entitymgr.LatticeAccountMgr;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupRequest;
import com.latticeengines.domain.exposed.datacloud.match.LatticeAccount;


public interface AccountLookupService {

    List<LatticeAccount> batchLookup(AccountLookupRequest request);
    LatticeAccountMgr getAccountMgr(String version);
    AccountLookupEntryMgr getLookupMgr(String version);

    List<String> batchLookupIds(AccountLookupRequest request);
    List<LatticeAccount> batchFetchAccounts(List<String> accountIds, String dataCloudVersion);

}
