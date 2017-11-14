package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.service.DnBCacheService;
import com.latticeengines.datacloud.core.service.NameLocationService;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupRequest;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.exposed.service.AccountLookupService;
import com.latticeengines.datacloud.match.service.DnBMatchResultValidator;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBCache;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.match.AccountLookupRequest;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;

@Component("dnbCacheLookupService")
public class DnBCacheLookupServiceImpl extends DataSourceLookupServiceBase {

    private static final Logger log = LoggerFactory.getLogger(DnBCacheLookupServiceImpl.class);

    @Autowired
    private NameLocationService nameLocationService;

    @Autowired
    private DnBCacheService dnbCacheService;

    @Autowired
    private AccountLookupService accountLookupService;

    @Autowired
    private DnBMatchResultValidator dnbMatchResultValidator;

    @Override
    protected Object lookupFromService(String lookupRequestId, DataSourceLookupRequest request) {
        MatchKeyTuple matchKeyTuple = (MatchKeyTuple) request.getInputData();
        DnBMatchContext context = new DnBMatchContext();
        context.setLookupRequestId(lookupRequestId);
        context.setInputNameLocation(matchKeyTuple);
        nameLocationService.setDefaultCountry(context.getInputNameLocation());
        context.setMatchStrategy(DnBMatchContext.DnBMatchStrategy.ENTITY);
        boolean readyToReturn = false;

        MatchTraveler traveler = request.getMatchTravelerContext();
        context.setDataCloudVersion(traveler.getDataCloudVersion());
        context.setRootOperationUid(traveler.getMatchInput().getRootOperationUid());
        if (!readyToReturn && traveler.getMatchInput().isUseDnBCache()) {
            Long startTime = System.currentTimeMillis();
            DnBCache cache = dnbCacheService.lookupCache(context);
            if (cache != null) {
                if (cache.isWhiteCache()) {
                    if ((request.getMatchTravelerContext().getMatchInput() != null
                            && request.getMatchTravelerContext().getMatchInput().isDisableDunsValidation())
                            || adoptWhiteCache(cache, context.getDataCloudVersion())) {
                        context.copyResultFromCache(cache);
                        dnbMatchResultValidator.validate(context);
                        log.info(String.format(
                                "Found DnB match context in white cache%s: Name=%s, Country=%s, State=%s, City=%s, "
                                        + "ZipCode=%s, PhoneNumber=%s, DUNS=%s, ConfidenceCode=%d, MatchGrade=%s, "
                                        + "OutOfBusiness=%s, IsDunsInAM=%s, Duration=%d",
                                context.getRootOperationUid() == null ? ""
                                        : " (RootOperationID=" + context.getRootOperationUid() + ")",
                                context.getInputNameLocation().getName(), context.getInputNameLocation().getCountry(),
                                context.getInputNameLocation().getState(), context.getInputNameLocation().getCity(),
                                context.getInputNameLocation().getZipcode(),
                                context.getInputNameLocation().getPhoneNumber(), context.getDuns(),
                                context.getConfidenceCode(), context.getMatchGrade().getRawCode(),
                                context.isOutOfBusinessString(), context.isDunsInAMString(),
                                System.currentTimeMillis() - startTime));
                    } else {
                        log.info(String.format(
                                "Reject invalid white cache: Id=%s DUNS=%s OutOfBusiness=%s IsDunsInAM=%s",
                                cache.getId(), cache.getDuns(), cache.isOutOfBusinessString(),
                                cache.isDunsInAMString()));
                    }
                } else {
                    context.copyResultFromCache(cache);
                    log.info(String.format(
                            "Found DnB match context in black cache%s: Name=%s, Country=%s, State=%s, City=%s, "
                                    + "ZipCode=%s, PhoneNumber=%s, Duration=%d",
                            context.getRootOperationUid() == null ? ""
                                    : " (RootOperationID=" + context.getRootOperationUid() + ")",
                            context.getInputNameLocation().getName(), context.getInputNameLocation().getCountry(),
                            context.getInputNameLocation().getState(), context.getInputNameLocation().getCity(),
                            context.getInputNameLocation().getZipcode(),
                            context.getInputNameLocation().getPhoneNumber(), System.currentTimeMillis() - startTime));
                }
            }
        }
        readyToReturn = true;
        context.setDuration(System.currentTimeMillis() - request.getTimestamp());
        return context;
    }

    @Override
    protected void asyncLookupFromService(String lookupRequestId, DataSourceLookupRequest request,
            String returnAddress) {
        DnBMatchContext result = (DnBMatchContext) lookupFromService(lookupRequestId, request);
        sendResponse(lookupRequestId, result, returnAddress);
    }

    private boolean adoptWhiteCache(DnBCache cache, String dataCloudVersion) {
        if (cache.isDunsInAM() == null) {
            return false;
        }
        if (Boolean.TRUE.equals(cache.isOutOfBusiness())) {
            return true;
        }
        boolean currentIsDunsInAM = isDunsInAM(cache.getDuns(), dataCloudVersion);
        if (currentIsDunsInAM == cache.isDunsInAM().booleanValue()) {
            return true; // If adopted cache has isDunsInAM == false, it will be
                         // discarded in DnBMatchResultValidator
        } else {
            return false;
        }
    }

    private boolean isDunsInAM(String duns, String dataCloudVersion) {
        AccountLookupRequest lookupRequest = new AccountLookupRequest(dataCloudVersion);
        lookupRequest.addLookupPair(null, duns);
        List<String> ids = accountLookupService.batchLookupIds(lookupRequest);
        return (ids != null && ids.size() == 1 && StringUtils.isNotEmpty(ids.get(0)));
    }

}
