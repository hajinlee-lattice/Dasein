package com.latticeengines.datacloud.match.exposed.service;

import java.util.List;

import com.latticeengines.datacloud.match.entitymgr.AccountLookupEntryMgr;
import com.latticeengines.datacloud.match.entitymgr.LatticeAccountMgr;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupRequest;
import com.latticeengines.domain.exposed.datacloud.match.LatticeAccount;


public interface AccountLookupService {

    List<String> batchLookupIds(AccountLookupRequest request);
    List<AccountLookupEntry> batchLookup(AccountLookupRequest request);
    List<LatticeAccount> batchFetchAccounts(List<String> accountIds, String dataCloudVersion);

    void updateLookupEntry(AccountLookupEntry lookupEntry, String dataCloudVersion);

    AccountLookupEntryMgr getLookupMgr(String version);
    LatticeAccountMgr getAccountMgr(String version);
}
