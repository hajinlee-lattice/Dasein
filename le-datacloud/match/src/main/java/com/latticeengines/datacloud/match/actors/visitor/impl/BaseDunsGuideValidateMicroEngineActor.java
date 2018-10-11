package com.latticeengines.datacloud.match.actors.visitor.impl;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.actors.visitor.MicroEngineActorTemplate;
import com.latticeengines.datacloud.match.service.DnBMatchPostProcessor;
import com.latticeengines.domain.exposed.datacloud.match.DunsGuideBook;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;

/*
 * Base actor that contains the DUNS redirection logic and delegate the post processing of the match result from
 * location-based actor to subclasses.
 */
public abstract class BaseDunsGuideValidateMicroEngineActor extends MicroEngineActorTemplate<DunsGuideBookLookupActor> {
    @Inject
    protected DnBMatchPostProcessor dnBMatchPostProcessor;

    @Override
    protected Class<DunsGuideBookLookupActor> getDataSourceActorClz() {
        return DunsGuideBookLookupActor.class;
    }

    @Override
    protected boolean accept(Traveler traveler) {
        MatchTraveler matchTraveler = (MatchTraveler) traveler;
        MatchKeyTuple matchKeyTuple = matchTraveler.getMatchKeyTuple();
        // have duns (from location-based actor) and a name/location based match key partition
        return matchKeyTuple.getDuns() != null && MatchKeyUtils.evalKeyPartition(matchKeyTuple) != null &&
                isInputDunsFromLocationBasedActor(matchTraveler);
    }

    @Override
    protected void process(Response response) {
        MatchTraveler traveler = (MatchTraveler) response.getTravelerContext();
        MatchKeyTuple tuple = traveler.getMatchKeyTuple();

        /* validate DnB match result (either cache/remote) */
        boolean isDunsInAM = response.getResult() != null;
        boolean isMatchResultValid = postProcessDnBMatchResult(traveler, isDunsInAM);
        if (!isDunsInAM || !isMatchResultValid) {
            // clear duns
            tuple.setDuns(null);
            return;
        }

        /* redirect (only when match result is valid and DUNS is in AM)  */

        DunsGuideBook book = (DunsGuideBook) response.getResult();
        if (CollectionUtils.isEmpty(book.getItems())) {
            // no duns to redirect to
            traveler.debug(String.format(
                    "DUNS=%s does not have target DUNS in guide book, no redirecting happens", tuple.getDuns()));
            return;
        }

        String dunsToRedirect = pickTargetDuns(tuple, book, traveler);
        if (!tuple.getDuns().equals(dunsToRedirect)) {
            // redirect to a different duns
            tuple.setDuns(dunsToRedirect);
            updateDunsOriginMap(traveler, dunsToRedirect);
        }
    }

    /**
     * Perform post processing (validation & updating cache/context) on DnB match result.
     *
     * @param traveler current match traveler
     * @param isDunsInAM flag to indicate whether the matched DUNS from location-based actor exists in AM
     * @return return false means DnB match result is NOT valid. return true means either 1. no need for post processing
     * or 2. post process is done and DnB match result is valid. the actor will not perform DUNS redirect if this method
     * return false
     */
    protected abstract boolean postProcessDnBMatchResult(@NotNull MatchTraveler traveler, boolean isDunsInAM);

    /*
     * pick a target DUNS to redirect to, return source DUNS if no valid target DUNS in the guide book.
     */
    private String pickTargetDuns(
            @NotNull MatchKeyTuple tuple, @NotNull DunsGuideBook book, @NotNull MatchTraveler traveler) {
        String keyPartition = getKeyPartition(tuple);
        String srcDuns = tuple.getDuns();

        for (DunsGuideBook.Item item : book.getItems()) {
            if (item == null || item.getKeyPartition() == null || item.getDuns() == null) {
                continue;
            }

            // first one that is more accurate than
            if (item.getKeyPartition().equals(keyPartition)) {
                traveler.debug(String.format("Redirect to target DUNS=%s, input KeyPartition=%s, BookSource=%s",
                        item.getDuns(), item.getKeyPartition(), item.getBookSource()));
                return item.getDuns();
            }
        }

        traveler.debug(String.format(
                "DUNS=%s does not have target DUNS by KeyPartition=%s in guide book, no redirecting happens",
                tuple.getDuns(), keyPartition));
        return srcDuns;
    }

    private void updateDunsOriginMap(MatchTraveler traveler, String duns) {
        traveler.setDunsOriginMapIfAbsent(new HashMap<>());
        traveler.getDunsOriginMap().put(this.getClass().getName(), duns);
    }

    /*
     * check if the input DUNS is from location-based actor (if not, its specified by the user with the
     * current implementation)
     *
     * traveler.getMatchKeyTuple().getDuns() is not null
     */
    private boolean isInputDunsFromLocationBasedActor(@NotNull MatchTraveler traveler) {
        String duns = traveler.getMatchKeyTuple().getDuns();

        Map<String, String> dunsOriginMap = traveler.getDunsOriginMap();
        if (MapUtils.isEmpty(dunsOriginMap)) {
            return false;
        }
        return duns.equals(dunsOriginMap.get(LocationToDunsMicroEngineActor.class.getName()))
                || duns.equals(dunsOriginMap.get(LocationToCachedDunsMicroEngineActor.class.getName()));
    }

    /*
     * when country is not specified by the user in the MatchKey and city/state is,
     * we will add USA as the default country, therefore we need to add country
     * when evaluating KeyPartition.
     *
     * NOTE input MatchKeyTuple will not be change by this function
     */
    private String getKeyPartition(@NotNull MatchKeyTuple tuple) {
        if (isCountryMatchKeyMissing(tuple)) {
            // add country match key for KeyPartition
            tuple.setCountryCode(MatchKey.Country.name());
            String keyPartition = MatchKeyUtils.evalKeyPartition(tuple);
            // clear country match key so that match key tuple does not change
            tuple.setCountryCode(null);
            return keyPartition;
        } else {
            return MatchKeyUtils.evalKeyPartition(tuple);
        }
    }

    /*
     * return true if country is missing and state/city is present
     */
    private boolean isCountryMatchKeyMissing(@NotNull MatchKeyTuple tuple) {
        return StringUtils.isBlank(tuple.getCountry())
                && (StringUtils.isNotBlank(tuple.getState()) || StringUtils.isNotBlank(tuple.getCity()));
    }
}
