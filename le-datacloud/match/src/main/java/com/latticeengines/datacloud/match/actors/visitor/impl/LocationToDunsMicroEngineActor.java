package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.apache.commons.collections4.MapUtils;
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

    private static final String REMOTE_API = "Went to remote DnB API.";

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

        // If already tried to get DUNS from LocationToCachedDunsActor or
        // LocationToDunsActor
        if (triedDunsFromLocation((MatchTraveler) traveler)) {
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

    private boolean triedDunsFromLocation(MatchTraveler traveler) {
        Map<String, String> dunsOriginMap = traveler.getDunsOriginMap();
        if (MapUtils.isEmpty(dunsOriginMap)) {
            return false;
        }
        if (dunsOriginMap.containsKey(this.getClass().getName())
                || dunsOriginMap.containsKey(LocationToCachedDunsMicroEngineActor.class.getName())) {
            return true;
        }
        return false;
    }

    @Override
    protected void process(Response response) {
        MatchTraveler traveler = (MatchTraveler) response.getTravelerContext();
        if (response.getResult() == null) {
            traveler.debug(String.format("Encountered an issue with DUNS lookup at %s: %s.", getClass().getSimpleName(),
                    "Result in response is empty"));
            return;
        }
        MatchKeyTuple matchKeyTuple = traveler.getMatchKeyTuple();
        DnBMatchContext res = (DnBMatchContext) response.getResult();
        traveler.debug(REMOTE_API);
        String logMessage = String.format(
                "Found DUNS=%s at %s. ConfidenceCode = %s, MatchGrade = %s. Matched Name = %s, Street = %s, City = %s, State = %s, CountryCode = %s, ZipCode = %s, PhoneNumber = %s, OutOfBusiness = %s.",
                res.getDuns(), getClass().getSimpleName(),
                (res.getConfidenceCode() == null ? null : res.getConfidenceCode().toString()),
                (res.getMatchGrade() == null ? null : res.getMatchGrade().getRawCode()),
                res.getMatchedNameLocation().getName(), res.getMatchedNameLocation().getStreet(),
                res.getMatchedNameLocation().getCity(), res.getMatchedNameLocation().getState(),
                res.getMatchedNameLocation().getCountryCode(), res.getMatchedNameLocation().getZipcode(),
                res.getMatchedNameLocation().getPhoneNumber(), res.isOutOfBusinessString());
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
        }
        traveler.setDunsOriginMapIfAbsent(new HashMap<>());
        traveler.getDunsOriginMap().put(this.getClass().getName(), res.getDuns());
        traveler.getDnBMatchContexts().add(res);
        response.setResult(null);
    }
}
