package com.latticeengines.datacloud.match.actors.visitor.impl;

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
import com.latticeengines.domain.exposed.datacloud.match.LdcMatchType;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;

@Component("locationBasedMicroEngineActor")
@Scope("prototype")
public class LocationToDunsMicroEngineActor extends DataSourceMicroEngineTemplate<DnbLookupActor> {
    private static final Logger log = LoggerFactory.getLogger(LocationToDunsMicroEngineActor.class);

    @PostConstruct
    public void postConstruct() {
        log.info("Started actor: " + self());
    }

    @Override
    protected Class<DnbLookupActor> getDataSourceActorClz() {
        return DnbLookupActor.class;
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
        if (!Boolean.TRUE.equals(input.getUseRemoteDnB())) {
            return false;
        }

        return StringUtils.isNotBlank(matchKeyTuple.getName());
    }

    @Override
    protected void recordActorAndTuple(MatchTraveler traveler) {
        traveler.setUseDunsMatchDuns(false);
        traveler.addEntityLdcMatchTypeToTupleList(Pair.of(LdcMatchType.LOCATION_DUNS, traveler.getMatchKeyTuple()));
    }

    private boolean triedDunsFromLocation(MatchTraveler traveler) {
        Map<String, String> dunsOriginMap = traveler.getDunsOriginMap();
        if (MapUtils.isEmpty(dunsOriginMap)) {
            return false;
        }
        return dunsOriginMap.containsKey(this.getClass().getName())
                || dunsOriginMap.containsKey(LocationToCachedDunsMicroEngineActor.class.getName());
    }

    @Override
    protected void process(Response response) {
        DnBResultProcessor.process(response, getClass());
    }
}
