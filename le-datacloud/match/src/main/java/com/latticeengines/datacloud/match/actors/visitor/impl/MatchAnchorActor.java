package com.latticeengines.datacloud.match.actors.visitor.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.ActorSystemTemplate;
import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.template.AnchorActorTemplate;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.framework.MatchGuideBook;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;

@Component("matchAnchorActor")
@Scope("prototype")
public class MatchAnchorActor extends AnchorActorTemplate {

    @Autowired
    @Qualifier("matchGuideBook")
    protected MatchGuideBook guideBook;

    @Autowired
    private MatchActorSystem matchActorSystem;

    @Override
    public GuideBook getGuideBook() {
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

    @Override
    protected boolean logCheckInNOut() {
        return false;
    }

}
