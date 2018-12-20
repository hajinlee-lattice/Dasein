package com.latticeengines.datacloud.match.actors.framework;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.ActorSystemTemplate;
import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.domain.exposed.datacloud.manage.DecisionGraph;

@Component("matchGuideBook")
public class MatchGuideBook extends GuideBook {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(MatchGuideBook.class);

    @Autowired
    private MatchActorSystem actorSystem;

    @Autowired
    private MatchDecisionGraphService matchDecisionGraphService;

    @Override
    protected ActorSystemTemplate getActorSystem() {
        return actorSystem;
    }

    @Override
    protected boolean stopTravel(Traveler traveler) {
        MatchTraveler matchTraveler = (MatchTraveler) traveler;
        return matchTraveler.isMatched();
    }

    @Override
    protected String getSerializedTravelerContext(Traveler traveler) {
        MatchTraveler matchTraveler = (MatchTraveler) traveler;
        return matchTraveler.getMatchKeyTuple().toString();
    }

    @Override
    protected DecisionGraph getDecisionGraphFromTraveler(Traveler traveler) throws Exception {
        MatchTraveler matchTraveler = (MatchTraveler) traveler;
        return matchDecisionGraphService.getDecisionGraph(matchTraveler);
    }

}
