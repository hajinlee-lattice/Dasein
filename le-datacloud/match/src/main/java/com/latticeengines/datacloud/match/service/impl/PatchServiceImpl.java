package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.core.service.DnBCacheService;
import com.latticeengines.datacloud.match.exposed.service.AccountLookupService;
import com.latticeengines.datacloud.match.exposed.service.PatchService;
import com.latticeengines.datacloud.match.exposed.service.RealTimeMatchService;
import com.latticeengines.datacloud.match.service.PublicDomainService;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBCache;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupEntry;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupRequest;
import com.latticeengines.domain.exposed.datacloud.match.LatticeAccount;
import com.latticeengines.domain.exposed.datacloud.match.LookupUpdateRequest;
import com.latticeengines.domain.exposed.datacloud.match.LookupUpdateResponse;
import com.latticeengines.domain.exposed.datacloud.match.MatchConstants;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("patchService")
public class PatchServiceImpl implements PatchService {

    private static final Logger log = LoggerFactory.getLogger(PatchServiceImpl.class);

    @Autowired
    private AccountLookupService accountLookupService;

    @Autowired
    private DataCloudVersionEntityMgr versionEntityMgr;

    @Autowired
    private RealTimeMatchService realTimeMatchService;

    @Autowired
    private PublicDomainService publicDomainService;

    @Autowired
    private DnBCacheService dnBCacheService;

    private String currentVersion;

    @PostConstruct
    public void postConstruct() {
        currentVersion = versionEntityMgr.currentApprovedVersion().getVersion();
    }

    @Override
    public LookupUpdateResponse patch(List<LookupUpdateRequest> updateRequests) {
        LookupUpdateResponse response = new LookupUpdateResponse();
        List<LookupUpdateResponse.Result> results = new ArrayList<>();
        for (LookupUpdateRequest request : updateRequests) {
            LookupUpdateResponse.Result result = new LookupUpdateResponse.Result();
            result.setRequest(request);
            try {
                patch(request);
                result.setSuccess(true);
            } catch (Exception e) {
                result.setSuccess(false);
                result.setErrorMessage(e.getMessage());
            }
            results.add(result);
        }
        response.setResults(results);
        return response;
    }

    @VisibleForTesting
    void patch(LookupUpdateRequest updateRequest) {
        verifyCanPath(updateRequest);

        OutputRecord outputRecord = match(updateRequest.getMatchKeys());
        verifyNeedToPatch(outputRecord, updateRequest.getLatticeAccountId());

        if (isDomainOnlyRequest(updateRequest)) {
            if (StringUtils.isNotEmpty(outputRecord.getPreMatchDomain())
                    && !publicDomainService.isPublicDomain(outputRecord.getPreMatchDomain())) {
                patchDomainOnly(outputRecord.getPreMatchDomain(), updateRequest.getLatticeAccountId());
                return;
            } else {
                throw new LedpException(LedpCode.LEDP_25035, new String[] { updateRequest.getMatchKeys().getDomain() });
            }
        }

        patchWithNameLocation(updateRequest, outputRecord);
    }

    @VisibleForTesting
    void verifyCanPath(LookupUpdateRequest updateRequest) {
        // must have lattice account id
        if (StringUtils.isEmpty(updateRequest.getLatticeAccountId())) {
            throw new LedpException(LedpCode.LEDP_25030);
        }

        // lattice account id must be valid
        String latticeAccountId = updateRequest.getLatticeAccountId();
        List<LatticeAccount> accountList = accountLookupService
                .batchFetchAccounts(Collections.singletonList(latticeAccountId), currentVersion);
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

    private void patchWithNameLocation(LookupUpdateRequest updateRequest, OutputRecord outputRecord) {
        String matchedDuns = outputRecord.getMatchedDuns();
        List<String> matchLogs = outputRecord.getMatchLogs();
        List<String> dnbCacheIds = outputRecord.getDnbCacheIds();
        String prematchedDomain = outputRecord.getPreMatchDomain();

        String duns = null;
        if (StringUtils.isNotEmpty(matchedDuns)) {
            String[] tokens = matchedDuns.split(",");
            if (tokens.length == 1) {
                duns = tokens[0];
            }
        }

        if (StringUtils.isEmpty(duns) && dnbCacheIds != null && dnbCacheIds.size() == 1) {
            String dnbCacheId = dnbCacheIds.get(0);
            DnBCache dnBCache = dnBCacheService.getCacheMgr().findByKey(dnbCacheId);
            if (dnBCache == null || dnBCache.getCacheContext() == null
                    || !dnBCache.getCacheContext().containsKey("Duns")
                    || StringUtils.isEmpty((String) dnBCache.getCacheContext().get("Duns"))) {
                if (matchLogs != null) {
                    log.info("Match Logs: \n" + StringUtils.join(matchLogs, "\n"));
                }
                throw new LedpException(LedpCode.LEDP_25036, new String[]{String.valueOf(matchedDuns)});
            } else {
                duns = (String) dnBCache.getCacheContext().get("Duns");
            }
            dnBCache.setPatched(true);
            dnBCacheService.getCacheMgr().create(dnBCache);
            log.info("Updated dnb cache entry with id=" + dnbCacheId + " to patched.");
        }

        if (StringUtils.isEmpty(duns)) {
            if (matchLogs != null) {
                log.info("Match Logs: \n" + StringUtils.join(matchLogs, "\n"));
            }
            throw new LedpException(LedpCode.LEDP_25036, new String[] { String.valueOf(matchedDuns) });
        }

        log.info("Duns to be patched is " + duns);

        AccountLookupEntry lookupEntry = new AccountLookupEntry();
        lookupEntry.setLatticeAccountId(updateRequest.getLatticeAccountId());
        lookupEntry.setDuns(duns);

        AccountLookupRequest request = new AccountLookupRequest(currentVersion);
        request.addLookupPair(null, duns);
        List<String> ids = accountLookupService.batchLookupIds(request);
        if (ids == null || StringUtils.isEmpty(ids.get(0))) {
            log.info("The to be patched duns does not exist in lookup table. Adding it");
            accountLookupService.updateLookupEntry(lookupEntry, currentVersion);
        }

        if (StringUtils.isNotEmpty(prematchedDomain) && !publicDomainService.isPublicDomain(prematchedDomain)) {
            request = new AccountLookupRequest(currentVersion);
            request.addLookupPair(prematchedDomain, duns);
            ids = accountLookupService.batchLookupIds(request);
            if (ids == null || StringUtils.isEmpty(ids.get(0))) {
                log.info("Patching domain + duns lookup.");
                lookupEntry.setDomain(prematchedDomain);
                accountLookupService.updateLookupEntry(lookupEntry, currentVersion);
            }
        } else {
            log.warn("Input domain is ignored. It is either an invalid domain or a public domain.");
        }
    }

    private OutputRecord match(MatchKeyTuple keyTuple) {
        MatchInput matchInput = prepareMatchInput(keyTuple);
        MatchOutput output = realTimeMatchService.match(matchInput);
        return output.getResult().get(0);
    }

    private void verifyNeedToPatch(OutputRecord outputRecord, String latticeAccountId) {
        if (outputRecord != null && StringUtils.isNotEmpty(outputRecord.getMatchedLatticeAccountId())) {
            String matchedId = outputRecord.getMatchedLatticeAccountId();
            if (latticeAccountId.equals(matchedId)) {
                throw new LedpException(LedpCode.LEDP_25034, new String[] { matchedId });
            }
        }
    }

    private MatchInput prepareMatchInput(MatchKeyTuple keyTuple) {
        MatchInput matchInput = new MatchInput();
        matchInput.setTenant(new Tenant(DataCloudConstants.SERVICE_TENANT));
        matchInput.setUseRemoteDnB(true);
        matchInput.setLogLevel(Level.DEBUG);
        matchInput.setDataCloudVersion(versionEntityMgr.currentApprovedVersionAsString());
        matchInput.setPredefinedSelection(ColumnSelection.Predefined.ID);
        List<Object> row = Arrays.asList(keyTuple.getDomain(), keyTuple.getName(), keyTuple.getCity(),
                keyTuple.getState(), keyTuple.getCountry(), keyTuple.getZipcode());
        List<List<Object>> data = Collections.singletonList(row);
        matchInput.setData(data);
        matchInput.setFields(Arrays.asList("Domain", "Name", "City", "State", "Country", "ZipCode"));
        return matchInput;
    }

}
