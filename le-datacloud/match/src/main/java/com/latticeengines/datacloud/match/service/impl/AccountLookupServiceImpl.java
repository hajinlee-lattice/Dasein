package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.entitymgr.AccountLookupEntryMgr;
import com.latticeengines.datacloud.match.entitymgr.LatticeAccountMgr;
import com.latticeengines.datacloud.match.entitymgr.impl.AccountLookupEntryMgrImpl;
import com.latticeengines.datacloud.match.entitymgr.impl.LatticeAccountMgrImpl;
import com.latticeengines.datacloud.match.exposed.service.AccountLookupService;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.message.FabricMessageService;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupRequest;
import com.latticeengines.domain.exposed.datacloud.match.LatticeAccount;


@Component("accountLookupService")
public class AccountLookupServiceImpl implements AccountLookupService {

    @SuppressWarnings("unused")
	private  static final Log log = LogFactory.getLog(AccountLookupServiceImpl.class);

    private Map<String, AccountLookupEntryMgr> lookupMgrs;
    private Map<String, LatticeAccountMgr> accountMgrs;

    @Autowired
    private FabricMessageService messageService;

    @Autowired
    private FabricDataService dataService;


    public AccountLookupServiceImpl() {
        lookupMgrs = new HashMap<String, AccountLookupEntryMgr>();
        accountMgrs = new HashMap<String, LatticeAccountMgr>();
    }


    @Override
    public List<LatticeAccount> batchLookup(AccountLookupRequest request) {
        String version = request.getVersion();

        AccountLookupEntryMgr lookupMgr = getLookupMgr(version);
        LatticeAccountMgr accountMgr = getAccountMgr(version);

        List<AccountLookupEntry> lookupEntries =  lookupMgr.batchFindByKey(request.getIds());

        List<String> accountIds = new ArrayList<String>();
        for (AccountLookupEntry entry : lookupEntries) {
            String id = (entry == null) ? null : entry.getLatticeAccountId();
            accountIds.add(id);
        }

        List<LatticeAccount> accounts = accountMgr.batchFindByKey(accountIds);

        return accounts;
    }

    public AccountLookupEntryMgr getLookupMgr(String version) {
        AccountLookupEntryMgr lookupMgr = lookupMgrs.get(version);
        if (lookupMgr == null)
            lookupMgr = getLookupMgrSync(version);
        return lookupMgr;
    }

    private synchronized AccountLookupEntryMgr getLookupMgrSync(String version) {
        AccountLookupEntryMgr lookupMgr = lookupMgrs.get(version);

        if (lookupMgr == null) {
            lookupMgr = new AccountLookupEntryMgrImpl(messageService, dataService, version);
            lookupMgr.init();
            lookupMgrs.put(version, lookupMgr);
        }

        return lookupMgr;
    }

    public LatticeAccountMgr getAccountMgr(String version) {
        LatticeAccountMgr accountMgr = accountMgrs.get(version);

        if (accountMgr == null)
            accountMgr = getAccountMgrSync(version);

        return accountMgr;
    }

    private synchronized LatticeAccountMgr getAccountMgrSync(String version) {
        LatticeAccountMgr accountMgr = accountMgrs.get(version);

        if (accountMgr == null) {
            accountMgr = new LatticeAccountMgrImpl(messageService, dataService, version);
            accountMgr.init();
            accountMgrs.put(version, accountMgr);
        }

        return accountMgr;
    }
}
