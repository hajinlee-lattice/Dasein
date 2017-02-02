package com.latticeengines.datacloud.match.service.impl;

import java.util.Collections;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.match.exposed.service.AccountLookupService;
import com.latticeengines.datacloud.match.exposed.service.PatchService;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.LatticeAccount;
import com.latticeengines.domain.exposed.datacloud.match.LookupUpdateRequest;
import com.latticeengines.domain.exposed.datacloud.match.MatchConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@Component("patchService")
public class PatchServiceImpl implements PatchService {

    @Autowired
    private AccountLookupService accountLookupService;

    @Autowired
    private DataCloudVersionEntityMgr versionEntityMgr;

    private String currentVersion;

    @PostConstruct
    public void postConstruct() {
        currentVersion = versionEntityMgr.currentApprovedVersion().getVersion();
    }

    @Override
    public void patch(LookupUpdateRequest updateRequest) {
        verifyCanPath(updateRequest);

        if (isDomainOnlyRequest(updateRequest)) {
            patchDomainOnly(updateRequest.getMatchKeys().getDomain(), updateRequest.getLatticeAccountId());
        }
    }

    @VisibleForTesting
    void verifyCanPath(LookupUpdateRequest updateRequest) {
        // must have lattice account id
        if (StringUtils.isEmpty(updateRequest.getLatticeAccountId())) {
            throw new LedpException(LedpCode.LEDP_25030);
        }

        // lattice account id must be valid
        String latticeAccountId = updateRequest.getLatticeAccountId();
        List<LatticeAccount> accountList = accountLookupService.batchFetchAccounts(
                Collections.singletonList(latticeAccountId), currentVersion);
        if (accountList == null || accountList.isEmpty() || accountList.get(0) == null) {
            throw new LedpException(LedpCode.LEDP_25031, new String[] { latticeAccountId });
        }
        LatticeAccount targetAccount = accountList.get(0);
        Object dunsObj = targetAccount.getAttributes().get(MatchConstants.AM_DUNS_FIELD);
        updateRequest.setTargetDuns(dunsObj == null ? null : dunsObj.toString());

        MatchKeyTuple keyTuple = updateRequest.getMatchKeys();

        // must provide non empty match keys
        if (keyTuple == null || (!keyTuple.hasDomain() && !keyTuple.hasLocation() && keyTuple.hasName())) {
            throw new LedpException(LedpCode.LEDP_25032);
        }

        // cannot be domain + location, without name
        if (keyTuple.hasDomain() && keyTuple.hasLocation() && !keyTuple.hasName()) {
            throw new LedpException(LedpCode.LEDP_25033);
        }

    }

    private boolean isDomainOnlyRequest(LookupUpdateRequest updateRequest) {
        MatchKeyTuple keyTuple = updateRequest.getMatchKeys();
        return keyTuple.hasDomain() && !keyTuple.hasName() && !keyTuple.hasLocation();
    }

    private void patchDomainOnly(String domain, String latticeAccountId) {
        AccountLookupEntry lookupEntry = new AccountLookupEntry();
        lookupEntry.setDomain(domain);
        lookupEntry.setLatticeAccountId(latticeAccountId);
        accountLookupService.updateLookupEntry(lookupEntry, currentVersion);
    }

}
