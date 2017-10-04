package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.actors.visitor.MicroEngineActorTemplate;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;

@Component("locationBasedMicroEngineActor")
@Scope("prototype")
public class LocationToDunsMicroEngineActor extends MicroEngineActorTemplate<DnbLookupActor> {
    private static final Logger log = LoggerFactory.getLogger(LocationToDunsMicroEngineActor.class);

    private static final String HIT_NO_CACHE = "Did not hit either white or black cache. Went to remote DnB API.";

    @PostConstruct
    public void postConstruct() {
        log.info("Started actor: " + self());
    }

    @Override
    protected Class<DnbLookupActor> getDataSourceActorClz() {
        return DnbLookupActor.class;
    }

    @Override
    protected boolean accept(Traveler traveler) {
        MatchKeyTuple matchKeyTuple = ((MatchTraveler) traveler).getMatchKeyTuple();
        Map<String, String> dunsOriginMap = ((MatchTraveler) traveler).getDunsOriginMap();

        if (matchKeyTuple.getDuns() != null && MapUtils.isNotEmpty(dunsOriginMap)
                && (dunsOriginMap.containsKey(this.getClass().getName())
                        || dunsOriginMap.containsKey(LocationToCachedDunsMicroEngineActor.class.getName()))) {
            // if DunsOriginMap has entry which was already made by this actor
            // class and current DUNS is not null then reject it
            return false;
        }

        MatchInput input = ((MatchTraveler) traveler).getMatchInput();
        if (!Boolean.TRUE.equals(input.getUseRemoteDnB())) {
            return false;
        }

        if (matchKeyTuple.getName() != null) {
            return true;
        }

        return false;
    }

    @Override
    protected void process(Response response) {
        if (response.getResult() != null) {
            MatchTraveler traveler = (MatchTraveler) response.getTravelerContext();
            MatchKeyTuple matchKeyTuple = traveler.getMatchKeyTuple();
            DnBMatchContext res = (DnBMatchContext) response.getResult();
            traveler.debug(HIT_NO_CACHE);
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
            }
            matchKeyTuple.setDuns(res.getDuns());
            Map<String, String> dunsOriginMap = ((MatchTraveler) traveler).getDunsOriginMap();
            if (dunsOriginMap == null) {
                dunsOriginMap = new HashMap<String, String>();
                ((MatchTraveler) traveler).setDunsOriginMap(dunsOriginMap);
            }
            dunsOriginMap.put(this.getClass().getName(), res.getDuns());
            traveler.getDnBMatchContexts().add(res);
            response.setResult(null);
        }
    }
}
