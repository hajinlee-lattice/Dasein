package com.latticeengines.datacloud.match.actors.visitor.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.ActorSystemTemplate;
import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.actors.template.ExecutorMicroEngineTemplate;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.framework.MatchGuideBook;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;

@Component("matchPlannerMicroEngineActor")
@Scope("prototype")
public class MatchPlannerMicroEngineActor extends ExecutorMicroEngineTemplate {

    @Autowired
    @Qualifier("matchActorSystem")
    protected MatchActorSystem matchActorSystem;

    @Autowired
    @Qualifier("matchGuideBook")
    protected MatchGuideBook guideBook;

    @Override
    protected GuideBook getGuideBook() {
        return guideBook;
    }

    @Override
    protected ActorSystemTemplate getActorSystem() {
        return matchActorSystem;
    }

    @Override
    protected boolean isValidMessageType(Object msg) {
        return msg instanceof MatchTraveler;
    }

    // TODO(@Jonathan): implement details
    @Override
    protected boolean accept(Traveler traveler) {
        @SuppressWarnings("unused") // TODO: remove this annotation after method
                                    // implementation is finished
        MatchTraveler matchTraveler = (MatchTraveler) traveler;
        return true;
    }

    // TODO(@Jonathan): change to property file to make it configurable & actual
    // number to be decided based on performance tuning
    @Override
    protected int getExecutorNum() {
        return 4;
    }

    // TODO(@Jonathan): implement details
    @Override
    protected void execute(Traveler traveler) {
        @SuppressWarnings("unused") // TODO: remove this annotation after method
                                    // implementation is finished
        MatchTraveler matchTraveler = (MatchTraveler) traveler;
    }

}
