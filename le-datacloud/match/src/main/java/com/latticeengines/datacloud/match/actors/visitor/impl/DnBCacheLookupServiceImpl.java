package com.latticeengines.datacloud.match.actors.visitor.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.core.service.DnBCacheService;
import com.latticeengines.datacloud.core.service.NameLocationService;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceLookupRequest;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.service.DnBMatchResultValidator;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBCache;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;

@Component("dnbCacheLookupService")
public class DnBCacheLookupServiceImpl extends DataSourceLookupServiceBase {

    private static final Logger log = LoggerFactory.getLogger(DnBCacheLookupServiceImpl.class);

    @Autowired
    private NameLocationService nameLocationService;

    @Autowired
    private DnBCacheService dnbCacheService;

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

        MatchTraveler traveler = request.getMatchTravelerContext();
        context.setDataCloudVersion(traveler.getDataCloudVersion());
        context.setRootOperationUid(traveler.getMatchInput().getRootOperationUid());
        if (traveler.getMatchInput().isUseDnBCache()) {
            long startTime = System.currentTimeMillis();
            DnBCache cache = dnbCacheService.lookupCache(context);
            if (cache != null) {
                if (cache.isWhiteCache()) {
                    if ((request.getMatchTravelerContext().getMatchInput() != null
                            && request.getMatchTravelerContext().getMatchInput().isDisableDunsValidation())
                            || adoptWhiteCache(cache)) {
                        context.copyResultFromCache(cache);

                        /*
                         * Ignore isDunsInAM field in the validator. This field will be validated in validator
                         * actor. This is because the check for current isDunsInAM value is moved to validator actor
                         * to improve performance (lookup requests there are batched)
                         */
                        Boolean isDunsInAM = context.isDunsInAM();
                        context.setDunsInAM(true);
                        dnbMatchResultValidator.validate(context);
                        // restore the original value
                        context.setDunsInAM(isDunsInAM);

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
        context.setDuration(System.currentTimeMillis() - request.getTimestamp());
        // Inject failure only for testing purpose
        injectFailure(request);
        return context;
    }

    @Override
    protected void asyncLookupFromService(String lookupRequestId, DataSourceLookupRequest request,
            String returnAddress) {
        DnBMatchContext result = (DnBMatchContext) lookupFromService(lookupRequestId, request);
        sendResponse(lookupRequestId, result, returnAddress);
    }

    private boolean adoptWhiteCache(DnBCache cache) {
        return cache.isDunsInAM() != null;
    }

}
