package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.datacloud.match.actors.visitor.DataSourceMicroEngineTemplate;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.domain.exposed.datacloud.match.EntityMatchType;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@Component("locationCacheBasedMicroEngineActor")
@Scope("prototype")
public class LocationToCachedDunsMicroEngineActor extends DataSourceMicroEngineTemplate<DnBCacheLookupActor> {
    private static final Logger log = LoggerFactory.getLogger(LocationToCachedDunsMicroEngineActor.class);

    private static final String HIT_WHITE_CACHE = "Retrieved a DUNS from white cache using Id=%s. Did not go to remote DnB API.";
    private static final String HIT_BLACK_CACHE = "Fast failed by black cache with Id=%s. Did not go to remote DnB API.";
    private static final String HIT_NO_CACHE = "Did not hit either white or black cache.";

    @PostConstruct
    public void postConstruct() {
        log.info("Started actor: " + self());
    }

    @Override
    protected Class<DnBCacheLookupActor> getDataSourceActorClz() {
        return DnBCacheLookupActor.class;
    }

    @Override
    protected boolean accept(MatchTraveler traveler) {
        MatchKeyTuple matchKeyTuple = traveler.getMatchKeyTuple();

        // If already tried to get DUNS from LocationToCachedDunsActor or
        // LocationToDunsActor
        if (triedDunsFromLocation(traveler)) {
            return false;
        }

        MatchInput input = traveler.getMatchInput();
        if (!Boolean.TRUE.equals(input.isUseDnBCache())) {
            return false;
        }

        if (matchKeyTuple.getName() != null) {

            // $JAW$
            traveler.addEntityLdcMatchTypeToTupleList(Pair.of(
                    EntityMatchType.LDC_LOCATION_CACHED_DUNS, traveler.getMatchKeyTuple()));
            return true;
        }

        return false;
    }

    private boolean triedDunsFromLocation(MatchTraveler traveler) {
        Map<String, String> dunsOriginMap = traveler.getDunsOriginMap();
        if (MapUtils.isEmpty(dunsOriginMap)) {
            return false;
        }
        if (dunsOriginMap.containsKey(this.getClass().getName())
                || dunsOriginMap.containsKey(LocationToDunsMicroEngineActor.class.getName())) {
            return true;
        }
        return false;
    }

    @Override
    protected void process(Response response) {
        MatchTraveler traveler = (MatchTraveler) response.getTravelerContext();
        if (response.getResult() == null) {
            traveler.debug(String.format("Encountered an issue with DUNS cache lookup at %s: %s.",
                    getClass().getSimpleName(), "Result in response is empty"));
            return;
        }
        MatchKeyTuple matchKeyTuple = traveler.getMatchKeyTuple();
        DnBMatchContext res = (DnBMatchContext) response.getResult();
        traveler.debug(res.getHitWhiteCache() ? String.format(HIT_WHITE_CACHE, res.getCacheId())
                : (res.getHitBlackCache() ? String.format(HIT_BLACK_CACHE, res.getCacheId()) : HIT_NO_CACHE));
        if (!res.getHitWhiteCache() && !res.getHitBlackCache()) {
            return;
        }
        String logMessage = String.format(
                "Found DUNS=%s at %s. ConfidenceCode = %s, MatchGrade = %s. Matched Name = %s, Street = %s, City = %s, State = %s, CountryCode = %s, ZipCode = %s, PhoneNumber = %s, OutOfBusiness = %s, IsDunsInAM = %s.",
                res.getDuns(), getClass().getSimpleName(),
                (res.getConfidenceCode() == null ? null : res.getConfidenceCode().toString()),
                (res.getMatchGrade() == null ? null : res.getMatchGrade().getRawCode()),
                res.getMatchedNameLocation().getName(), res.getMatchedNameLocation().getStreet(),
                res.getMatchedNameLocation().getCity(), res.getMatchedNameLocation().getState(),
                res.getMatchedNameLocation().getCountryCode(), res.getMatchedNameLocation().getZipcode(),
                res.getMatchedNameLocation().getPhoneNumber(), res.isOutOfBusinessString(), res.isDunsInAMString());
        if (Boolean.TRUE.equals(res.getPatched())) {
            logMessage += " This cache entry has been manually patched.";
        }
        traveler.debug(logMessage);
        if (res.getDnbCode() != DnBReturnCode.OK) {
            if (StringUtils.isNotEmpty(res.getDuns())) {
                res.setDuns(null);
            }
            traveler.debug(String.format("Encountered an issue with DUNS lookup at %s: %s.", //
                    getClass().getSimpleName(), //
                    (res.getDnbCode() == null ? "No DnBReturnCode" : res.getDnbCode().getMessage())));
        } else {
            matchKeyTuple.setDuns(res.getDuns());

            // $JAW$ Match Report
            traveler.addEntityMatchLookupResults(BusinessEntity.LatticeAccount.name() + "_LTCD",
                    Collections.singletonList(Pair.of(traveler.getMatchKeyTuple(),
                            Collections.singletonList(traveler.getLatticeAccountId()))));
        }
        traveler.setDunsOriginMapIfAbsent(new HashMap<>());
        traveler.getDunsOriginMap().put(this.getClass().getName(), res.getDuns());
        traveler.getDnBMatchContexts().add(res);
        response.setResult(null);
    }
}
