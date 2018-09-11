package com.latticeengines.datacloud.match.actors.visitor.impl;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.actors.visitor.MicroEngineActorTemplate;
import com.latticeengines.domain.exposed.datacloud.match.DunsGuideBook;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyUtils;
import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

@Component("dunsValidatorMicroEngineActor")
@Scope("prototype")
public class DunsValidatorMicroEngineActor extends MicroEngineActorTemplate<DunsGuideBookLookupActor> {
    private static final Logger log = LoggerFactory.getLogger(DunsValidatorMicroEngineActor.class);

    @PostConstruct
    public void postConstruct() {
        log.info("Started actor: " + self());
    }

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
        if (response.getResult() == null) {
            traveler.debug(String.format("DUNS=%s does not exist in AM, discard it", tuple.getDuns()));
            // DunsGuideBook does not exist => DUNS not in AM
            // clear duns
            tuple.setDuns(null);
            return;
        }


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

    /*
     * pick a target DUNS to redirect to, return source DUNS if no valid target DUNS in the guide book.
     */
    private String pickTargetDuns(@NotNull MatchKeyTuple tuple, @NotNull DunsGuideBook book, @NotNull MatchTraveler traveler) {
        String keyPartition = MatchKeyUtils.evalKeyPartition(tuple);
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
        if (traveler.getDunsOriginMap() == null) {
            traveler.setDunsOriginMap(new HashMap<>());
        }
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
}
