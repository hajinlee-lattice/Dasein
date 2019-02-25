package com.latticeengines.datacloud.match.service.impl;

import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.google.common.base.Preconditions;
import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.core.service.DnBCacheService;
import com.latticeengines.datacloud.match.actors.visitor.impl.LocationToCachedDunsMicroEngineActor;
import com.latticeengines.datacloud.match.service.DnBMatchPostProcessor;
import com.latticeengines.datacloud.match.service.DnBMatchResultValidator;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBCache;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;

@Component("dnBMatchPostProcessor")
public class DnBMatchPostProcessorImpl implements DnBMatchPostProcessor {

    @Inject
    private DnBMatchResultValidator validator;

    @Inject
    private DnBCacheService dnBCacheService;

    @Override
    public boolean shouldPostProcessCacheResult(DnBMatchContext context, MatchInput input, MatchKeyTuple tuple) {
        if (!shouldProcess(context, input, tuple)) {
            return false;
        }

        if (!input.isUseDnBCache() || input.isDisableDunsValidation()) {
            return false;
        }

        return Boolean.TRUE.equals(context.getHitWhiteCache());
    }

    @Override
    public boolean shouldPostProcessRemoteResult(DnBMatchContext context, MatchInput input, MatchKeyTuple tuple) {
        if (!shouldProcess(context, input, tuple)) {
            return false;
        }

        if (!Boolean.TRUE.equals(input.getUseRemoteDnB())) {
            return false;
        }

        return context.isCalledRemoteDnB();
    }

    @Override
    public boolean postProcessCacheResult(
            Traveler traveler, DnBMatchContext context, Map<String, String> dunsOriginMap, boolean currentIsDunsInAM) {
        Preconditions.checkNotNull(context);
        Preconditions.checkNotNull(dunsOriginMap);
        Preconditions.checkArgument(Boolean.TRUE.equals(context.getHitWhiteCache()));
        if (adoptWhiteCache(context, currentIsDunsInAM)) {
            if (Boolean.FALSE.equals(context.isDunsInAM())) {
                // in LocationToCachedDuns actor, when adopting white cache, isDunsInAM field is not validated
                // because we cannot discard cache.isDunsInAM == false entries until we check whether the DUNS is currently
                // in AM. There can be the case where cache.isDunsInAM == false && currentIsDunsInAM == true and we
                // will need to refresh the cache. So do the validation on context.isDunsInAM (same as cache.isDunsInAM)
                // and discard if the entry is not valid
                validator.validate(context);
                if (!DnBReturnCode.OK.equals(context.getDnbCode()) && StringUtils.isNotBlank(context.getDuns())) {
                    traveler.debug(String.format("DUNS=%s does not exist in AM, discard it", context.getDuns()));
                    context.setDuns(null);
                }
            }
        } else {
            // since we adopted the cache by default, we need to reverse the effect
            context.clearWhiteCacheResult();
            // clear DUNS origin map when adopting cache so that it will going to remote DnB API to refresh cache entry
            // NOTE really don't want service to have knowledge of any actor, but haven't find a better choice
            dunsOriginMap.remove(LocationToCachedDunsMicroEngineActor.class.getName());
            traveler.debug(String.format(
                    "Reject invalid white cache: Id=%s DUNS=%s OutOfBusiness=%s IsDunsInAM=%s CurrentIsDunsInAM=%s",
                    context.getCacheId(), context.getDuns(), context.isOutOfBusinessString(),
                    context.isDunsInAMString(), currentIsDunsInAM));
        }
        // if context.getDuns() is not cleared means the result is valid
        return context.getDuns() != null;
    }

    @Override
    public boolean postProcessRemoteResult(
            Traveler traveler, DnBMatchContext context, Map<String, String> dunsOriginMap, boolean currentIsDunsInAM) {
        Preconditions.checkNotNull(context);
        Preconditions.checkNotNull(context.getDuns());
        Preconditions.checkArgument(Boolean.TRUE.equals(context.isCalledRemoteDnB()));

        if (!currentIsDunsInAM) {
            // when current DUNS is not in AM, the validation done in LocationToDuns actor is on the wrong isDunsInAM
            // value, need to validate again
            context.setDunsInAM(false);
            validator.validate(context);
            DnBCache dnBCache = dnBCacheService.addCache(context, false);
            if (dnBCache != null) {
                traveler.debug(String.format(
                        "Set isDunsInAM to false for DnBCache Id=%s DUNS=%s",
                        dnBCache.getId(), context.getDuns()));
                context.setCacheId(dnBCache.getId());
            }
            if (!DnBReturnCode.OK.equals(context.getDnbCode()) && StringUtils.isNotBlank(context.getDuns())) {
                traveler.debug(String.format("DUNS=%s does not exist in AM, discard it", context.getDuns()));
                context.setDuns(null);
            }
        }

        return currentIsDunsInAM;
    }

    /*
     * basic checks
     */
    private boolean shouldProcess(DnBMatchContext context, MatchInput input, MatchKeyTuple tuple) {
        if (context == null || input == null || tuple == null) {
            return false;
        }
        if (tuple.getDuns() == null || context.getDuns() == null) {
            return false;
        }
        return tuple.getDuns().equals(context.getDuns());
    }

    private boolean adoptWhiteCache(@NotNull DnBMatchContext context, boolean currentIsDunsInAM) {
        if (context.isDunsInAM() == null) {
            return false;
        }
        if (Boolean.TRUE.equals(context.isOutOfBusiness())) {
            return true;
        }
        // if isDunsInAM == false, it will be discarded later
        return context.isDunsInAM() == currentIsDunsInAM;
    }
}
