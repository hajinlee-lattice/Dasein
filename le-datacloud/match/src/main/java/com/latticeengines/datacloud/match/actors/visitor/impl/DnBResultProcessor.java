package com.latticeengines.datacloud.match.actors.visitor.impl;

import java.util.Collections;
import java.util.HashMap;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.template.VisitorActorTemplate;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchCandidate;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.query.BusinessEntity;

final class DnBResultProcessor {

    private static final Logger log = LoggerFactory.getLogger(DnBResultProcessor.class);

    protected DnBResultProcessor() {
        throw new UnsupportedOperationException();
    }

    static void process(Response response, Class<? extends VisitorActorTemplate> actorClz) {
        MatchTraveler traveler = (MatchTraveler) response.getTravelerContext();
        if (response.getResult() == null) {
            traveler.debug(String.format("Encountered an issue with DUNS lookup at %s: %s.", actorClz.getSimpleName(),
                    "Result in response is empty"));
            traveler.addEntityMatchLookupResults(BusinessEntity.LatticeAccount.name(),
                    Collections.singletonList(Pair.of(traveler.getMatchKeyTuple(),
                            Collections.singletonList(null))));
            return;
        }
        MatchKeyTuple matchKeyTuple = traveler.getMatchKeyTuple();
        DnBMatchContext res = (DnBMatchContext) response.getResult();
        traveler.debug("Went to remote DnB API.");
        String logMessage = String.format(
                "Found DUNS=%s at %s. ConfidenceCode = %s, MatchGrade = %s. Matched Name = %s, Street = %s, " +
                        "City = %s, State = %s, CountryCode = %s, ZipCode = %s, PhoneNumber = %s, OutOfBusiness = %s.",
                res.getDuns(), actorClz.getSimpleName(),
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
        traveler.setDunsOriginMapIfAbsent(new HashMap<>());
        traveler.getDunsOriginMap().put(actorClz.getName(), res.getDuns());
        traveler.getDnBMatchContexts().add(res);
        traveler.addEntityMatchLookupResults(BusinessEntity.LatticeAccount.name(),
                Collections.singletonList(Pair.of(traveler.getMatchKeyTuple(),
                        Collections.singletonList(res.getDuns()))));
        if (res.getDnbCode() != DnBReturnCode.OK) {
            if (StringUtils.isNotEmpty(res.getDuns())) {
                res.setDuns(null);
                if (BusinessEntity.PrimeAccount.name().equals(traveler.getEntity())) {
                    traveler.setCandidates(res.getCandidates());
                    traveler.setMatched(false);
                }
            }
            traveler.debug(String.format("Encountered an issue with DUNS lookup at %s: %s.", //
                    actorClz.getSimpleName(), //
                    (res.getDnbCode() == null ? "No DnBReturnCode" : res.getDnbCode().getMessage())));
        } else {
            if (StringUtils.isNotBlank(res.getDuns())) {
                matchKeyTuple.setDuns(res.getDuns());
            }
            if (BusinessEntity.PrimeAccount.name().equals(traveler.getEntity())) {
                traveler.setCandidates(res.getCandidates());
                String duns = parseMatchedDuns(res);
                traveler.setMatched(StringUtils.isNotBlank(duns));
                traveler.setResult(duns);
            }
        }
        response.setResult(null);
    }

    private static String parseMatchedDuns(DnBMatchContext res) {
        if (CollectionUtils.isNotEmpty(res.getCandidates())) {
            DnBMatchCandidate candidate = res.getCandidates().get(0);
            if (candidate != null && StringUtils.isNotBlank(candidate.getDuns())) {
                return candidate.getDuns();
            }
        }
        return null;
    }

}
