package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.match.entitymgr.AccountLookupEntryMgr;
import com.latticeengines.datacloud.match.entitymgr.LatticeAccountMgr;
import com.latticeengines.datacloud.match.entitymgr.impl.AccountLookupEntryMgrImpl;
import com.latticeengines.datacloud.match.entitymgr.impl.LatticeAccountMgrImpl;
import com.latticeengines.datacloud.match.exposed.service.AccountLookupService;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.datafabric.service.message.FabricMessageService;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupRequest;
import com.latticeengines.domain.exposed.datacloud.match.LatticeAccount;


@Component("accountLookupService")
public class AccountLookupServiceImpl implements AccountLookupService {

	private  static final Log log = LogFactory.getLog(AccountLookupServiceImpl.class);

    private Map<String, AccountLookupEntryMgr> lookupMgrs;
    private Map<String, LatticeAccountMgr> accountMgrs;

    @Autowired
    private FabricMessageService messageService;

    @Autowired
    private FabricDataService dataService;

    @Autowired
    private DataCloudVersionEntityMgr versionEntityMgr;

    public AccountLookupServiceImpl() {
        lookupMgrs = new HashMap<>();
        accountMgrs = new HashMap<>();
    }


    @Override
    public List<LatticeAccount> batchLookup(AccountLookupRequest request) {
        List<String> accountIds = batchLookupIds(request);
        return batchFetchAccounts(accountIds, request.getVersion());
    }

    @Override
    public List<String> batchLookupIds(AccountLookupRequest request) {
        String version = request.getVersion();
        AccountLookupEntryMgr lookupMgr = getLookupMgr(version);
        List<AccountLookupEntry> lookupEntries =  lookupMgr.batchFindByKey(request.getIds());

        List<String> accountIds = new ArrayList<>();
        for (AccountLookupEntry entry : lookupEntries) {
            String id = (entry == null) ? null : entry.getLatticeAccountId();
            accountIds.add(id);
        }

        return accountIds;
    }

    @Override
    public List<LatticeAccount> batchFetchAccounts(List<String> accountIds, String dataCloudVersion) {
        LatticeAccountMgr accountMgr = getAccountMgr(dataCloudVersion);
        return accountMgr.batchFindByKey(accountIds);
    }

    @Override
    public AccountLookupEntryMgr getLookupMgr(String version) {
        AccountLookupEntryMgr lookupMgr = lookupMgrs.get(version);
        if (lookupMgr == null)
            lookupMgr = getLookupMgrSync(version);
        return lookupMgr;
    }

    private synchronized AccountLookupEntryMgr getLookupMgrSync(String version) {
        AccountLookupEntryMgr lookupMgr = lookupMgrs.get(version);

        if (lookupMgr == null) {
            DataCloudVersion dataCloudVersion = versionEntityMgr.findVersion(version);
            if (dataCloudVersion == null) {
                throw new IllegalArgumentException("Cannot find the specified data cloud version " + version);
            }
            String signature = dataCloudVersion.getDynamoTableSignature();
            String fullVersion = version;
            if (StringUtils.isNotEmpty(signature)) {
                fullVersion += "_" + signature;
            }
            log.info("Use " + fullVersion + " as full version of AccountLookup for " + version);
            lookupMgr = new AccountLookupEntryMgrImpl(messageService, dataService, fullVersion);
            lookupMgr.init();
            lookupMgrs.put(version, lookupMgr);
        }

        return lookupMgr;
    }

    @Override
    public LatticeAccountMgr getAccountMgr(String version) {
        LatticeAccountMgr accountMgr = accountMgrs.get(version);

        if (accountMgr == null)
            accountMgr = getAccountMgrSync(version);

        return accountMgr;
    }

    private synchronized LatticeAccountMgr getAccountMgrSync(String version) {
        LatticeAccountMgr accountMgr = accountMgrs.get(version);
        if (accountMgr == null) {
            DataCloudVersion dataCloudVersion = versionEntityMgr.findVersion(version);
            if (dataCloudVersion == null) {
                throw new IllegalArgumentException("Cannot find the specified data cloud version " + version);
            }
            String signature = dataCloudVersion.getDynamoTableSignature();
            String fullVersion = version;
            if (StringUtils.isNotEmpty(signature)) {
                fullVersion += "_" + signature;
            }
            log.info("Use " + fullVersion + " as full version of LatticeAccount for " + version);
            accountMgr = new LatticeAccountMgrImpl(messageService, dataService, fullVersion);
            accountMgr.init();
            accountMgrs.put(version, accountMgr);
        }

        return accountMgr;
    }
}
