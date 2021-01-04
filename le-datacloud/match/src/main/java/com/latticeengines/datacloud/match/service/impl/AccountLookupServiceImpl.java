package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.core.service.DataCloudVersionService;
import com.latticeengines.datacloud.match.entitymgr.AccountLookupEntryMgr;
import com.latticeengines.datacloud.match.entitymgr.LatticeAccountMgr;
import com.latticeengines.datacloud.match.entitymgr.impl.AccountLookupEntryMgrImpl;
import com.latticeengines.datacloud.match.entitymgr.impl.LatticeAccountMgrImpl;
import com.latticeengines.datacloud.match.exposed.service.AccountLookupService;
import com.latticeengines.datafabric.service.datastore.FabricDataService;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupRequest;
import com.latticeengines.domain.exposed.datacloud.match.LatticeAccount;

@Component("accountLookupService")
public class AccountLookupServiceImpl implements AccountLookupService {

    private static final Logger log = LoggerFactory.getLogger(AccountLookupServiceImpl.class);

    private Map<String, AccountLookupEntryMgr> lookupMgrs;
    private Map<String, LatticeAccountMgr> accountMgrs;

    @Inject
    private FabricDataService dataService;

    @Inject
    private DataCloudVersionEntityMgr versionEntityMgr;

    @Inject
    private DataCloudVersionService versionService;

    public AccountLookupServiceImpl() {
        lookupMgrs = new HashMap<>();
        accountMgrs = new HashMap<>();
    }

    @Override
    public List<String> batchLookupIds(AccountLookupRequest request) {
        String version = request.getVersion();
        AccountLookupEntryMgr lookupMgr = getLookupMgr(version);
        List<AccountLookupEntry> lookupEntries = lookupMgr.batchFindByKey(request.getIds());

        List<String> accountIds = new ArrayList<>();
        for (AccountLookupEntry entry : lookupEntries) {
            String id = (entry == null) ? null : entry.getLatticeAccountId();
            accountIds.add(id);
        }

        return accountIds;
    }

    @Override
    public List<AccountLookupEntry> batchLookup(AccountLookupRequest request) {
        String version = request.getVersion();
        AccountLookupEntryMgr lookupMgr = getLookupMgr(version);
        return lookupMgr.batchFindByKey(request.getIds());
    }

    @Override
    public List<LatticeAccount> batchFetchAccounts(List<String> accountIds,
            String dataCloudVersion) {
        LatticeAccountMgr accountMgr = getAccountMgr(dataCloudVersion);
        return accountMgr.batchFindByKey(accountIds);
    }

    @Override
    public void updateLookupEntry(AccountLookupEntry lookupEntry, String dataCloudVersion) {
        AccountLookupEntryMgr lookupMgr = getLookupMgr(dataCloudVersion);
        String lookupId = lookupEntry.getId();
        if (StringUtils.isEmpty(lookupId)) {
            throw new RuntimeException("Must provide Id in the lookup entity to be updated.");
        }
        String accountId = lookupEntry.getLatticeAccountId();
        if (StringUtils.isEmpty(accountId)) {
            throw new RuntimeException(
                    "Must provide LatticeAccountId in the lookup entity to be updated.");
        }
        AccountLookupEntry lookupEntryInDynamo = lookupMgr.findByKey(lookupId);
        if (lookupEntryInDynamo != null) {
            lookupMgr.update(lookupEntry);
            log.info(
                    "Updated lookup from " + lookupId + " to " + lookupEntry.getLatticeAccountId());
        } else {
            lookupMgr.create(lookupEntry);
            log.info(
                    "Created lookup from " + lookupId + " to " + lookupEntry.getLatticeAccountId());
        }
    }

    @Override
    public AccountLookupEntryMgr getLookupMgr(String version) {
        AccountLookupEntryMgr lookupMgr = lookupMgrs.get(version);
        if (lookupMgr == null) {
            lookupMgr = getLookupMgrSync(version);
        }
        return lookupMgr;
    }

    private synchronized AccountLookupEntryMgr getLookupMgrSync(String version) {
        AccountLookupEntryMgr lookupMgr = lookupMgrs.get(version);

        if (lookupMgr == null) {
            DataCloudVersion dataCloudVersion = versionEntityMgr.findVersion(version);
            if (dataCloudVersion == null) {
                throw new IllegalArgumentException(
                        "Cannot find the specified data cloud version " + version);
            }
            String signature = dataCloudVersion.getDynamoTableSignatureLookup();
            String fullVersion = versionService.constructDynamoVersion(version, signature);
            log.info("Use " + fullVersion + " as full version of AccountLookup for " + version);
            lookupMgr = new AccountLookupEntryMgrImpl(dataService, fullVersion);
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
                throw new IllegalArgumentException(
                        "Cannot find the specified data cloud version " + version);
            }
            String signature = dataCloudVersion.getDynamoTableSignature();
            String fullVersion = versionService.constructDynamoVersion(version, signature);
            log.info("Use " + fullVersion + " as full version of LatticeAccount for " + version);
            accountMgr = new LatticeAccountMgrImpl(dataService, fullVersion);
            accountMgr.init();
            accountMgrs.put(version, accountMgr);
        }

        return accountMgr;
    }
}
