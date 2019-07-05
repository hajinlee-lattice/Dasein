package com.latticeengines.datacloud.match.service.impl;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.core.entitymgr.DnBCacheEntityMgr;
import com.latticeengines.datacloud.core.service.DnBCacheService;
import com.latticeengines.datacloud.match.entitymgr.AccountLookupEntryMgr;
import com.latticeengines.datacloud.match.entitymgr.DecisionGraphEntityMgr;
import com.latticeengines.datacloud.match.entitymgr.DunsGuideBookEntityMgr;
import com.latticeengines.datacloud.match.entitymgr.LatticeAccountMgr;
import com.latticeengines.datacloud.match.exposed.service.AccountLookupService;
import com.latticeengines.datacloud.match.exposed.service.DunsGuideBookService;
import com.latticeengines.datacloud.match.exposed.service.MatchValidationService;
import com.latticeengines.datacloud.match.exposed.util.MatchUtils;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("matchValidationService")
public class MatchValidationServiceImpl implements MatchValidationService {

    @Inject
    private DecisionGraphEntityMgr decisionGraphEntityMgr;

    @Inject
    private DnBCacheService dnbCacheService;

    @Inject
    private AccountLookupService accountLookupService;

    @Inject
    private DunsGuideBookService dunsGuideBookService;

    @Value("${datacloud.match.default.decision.graph}")
    private String defaultDecisionGraph;

    @Override
    public void validateDecisionGraph(String decisionGraph) {
        if (decisionGraph == null) {
            decisionGraph = defaultDecisionGraph;
        }
        if (decisionGraphEntityMgr.getDecisionGraph(decisionGraph) == null) {
            String errorMsg = String.format("DecisionGraph = %s does not exist", decisionGraph);
            throw new IllegalArgumentException(errorMsg);
        }
    }

    @Override
    public void validateDefaultDecisionGraph() {
        validateDecisionGraph(null);
    }

    @Override
    public void validateDataCloudVersion(@NotNull String dataCloudVersion, @NotNull Tenant tenant) {
        // 1.0 SQL table based matcher is no longer supported as SQL Server in
        // BODC is shutdown. Fail 1.0 match request.
        if (MatchUtils.isValidForRTSBasedMatch(dataCloudVersion)) {
            String errorMsg = "Found a match request against RTS source which is unsupported any more. Tenant="
                    + JsonUtils.serialize(tenant);
            throw new IllegalArgumentException(errorMsg);
        }
        checkDataFabricEntityMgrs(dataCloudVersion);
    }

    /*
     * check whether all fabric entity managers are valid for the given datacloud version
     *
     * 1. DnBCache
     * 2. AccountLookup
     * 3. LatticeAccount
     * 4. DunsGuideBook
     */
    private void checkDataFabricEntityMgrs(String dataCloudVersion) {
        checkDnBCacheEntityMgr(dataCloudVersion);
        checkAccountLookupEntityMgr(dataCloudVersion);
        checkDunsGuideBookEntityMgr(dataCloudVersion);
    }

    private void checkDunsGuideBookEntityMgr(String dataCloudVersion) {
        if (!MatchUtils.isValidForAccountMasterBasedMatch(dataCloudVersion)) {
            return;
        }

        DunsGuideBookEntityMgr entityMgr = dunsGuideBookService.getEntityMgr(dataCloudVersion);
        if (entityMgr == null || entityMgr.isDisabled()) {
            throw new IllegalStateException("DunsGuideBookEntityMgr is disabled.");
        }
    }

    private void checkDnBCacheEntityMgr(String dataCloudVersion) {
        if (MatchUtils.isValidForAccountMasterBasedMatch(dataCloudVersion)) {
            DnBCacheEntityMgr entityMgr = dnbCacheService.getCacheMgr();
            if (entityMgr == null || entityMgr.isDisabled()) {
                throw new RuntimeException("DnBCacheEntityMgr is disabled.");
            }
        }
    }

    private void checkAccountLookupEntityMgr(String dataCloudVersion) {
        if (MatchUtils.isValidForAccountMasterBasedMatch(dataCloudVersion)) {
            AccountLookupEntryMgr lookupEntityMgr = accountLookupService.getLookupMgr(dataCloudVersion);
            if (lookupEntityMgr == null || lookupEntityMgr.isDisabled()) {
                throw new RuntimeException("AccountLookupEntryMgr is disabled.");
            }
            LatticeAccountMgr latticeAccountMgr = accountLookupService.getAccountMgr(dataCloudVersion);
            if (latticeAccountMgr == null || latticeAccountMgr.isDisabled()) {
                throw new RuntimeException("LatticeAccountMgr is disabled.");
            }
        }
    }
}
