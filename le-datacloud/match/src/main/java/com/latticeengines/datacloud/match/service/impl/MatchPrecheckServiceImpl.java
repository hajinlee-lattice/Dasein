package com.latticeengines.datacloud.match.service.impl;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.entitymgr.DnBCacheEntityMgr;
import com.latticeengines.datacloud.core.service.DnBCacheService;
import com.latticeengines.datacloud.match.entitymgr.AccountLookupEntryMgr;
import com.latticeengines.datacloud.match.entitymgr.LatticeAccountMgr;
import com.latticeengines.datacloud.match.exposed.service.AccountLookupService;
import com.latticeengines.datacloud.match.exposed.service.MatchPrecheckService;

@Component("matchPrecheckService")
public class MatchPrecheckServiceImpl implements MatchPrecheckService {
    @Autowired
    private DnBCacheService dnbCacheService;

    @Autowired
    private AccountLookupService accountLookupService;

    @Override
    public void precheck(String matchVersion) {
        checkDataFabricEntityMgr(matchVersion);
    }

    private void checkDataFabricEntityMgr(String matchVersion) {
        checkDnBCacheEntityMgr(matchVersion);
        checkAccountLookupEntityMgr(matchVersion);
    }

    private void checkDnBCacheEntityMgr(String matchVersion) {
        if (StringUtils.isEmpty(matchVersion) || matchVersion.startsWith("1")) {
            return;
        }
        DnBCacheEntityMgr entityMgr = dnbCacheService.getCacheMgr();
        if (entityMgr == null || entityMgr.isDisabled()) {
            throw new RuntimeException("DnBCacheEntityMgr is disabled.");
        }
    }
    
    private void checkAccountLookupEntityMgr(String matchVersion) {
        if (StringUtils.isEmpty(matchVersion) || matchVersion.startsWith("1")) {
            return;
        }
        AccountLookupEntryMgr lookupEntityMgr = accountLookupService.getLookupMgr(matchVersion);
        if (lookupEntityMgr == null || lookupEntityMgr.isDisabled()) {
            throw new RuntimeException("AccountLookupEntryMgr is disabled.");
        }
        LatticeAccountMgr latticeAccountMgr = accountLookupService.getAccountMgr(matchVersion);
        if (latticeAccountMgr == null || latticeAccountMgr.isDisabled()) {
            throw new RuntimeException("LatticeAccountMgr is disabled.");
        }
    }
}
