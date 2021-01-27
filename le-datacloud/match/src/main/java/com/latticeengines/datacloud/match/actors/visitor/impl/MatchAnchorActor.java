package com.latticeengines.datacloud.match.actors.visitor.impl;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.exposed.ActorSystemTemplate;
import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.actors.template.AnchorActorTemplate;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.framework.MatchGuideBook;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.service.EntityMatchConfigurationService;

@Component("matchAnchorActor")
@Scope("prototype")
public class MatchAnchorActor extends AnchorActorTemplate {

    @Resource(name = "matchGuideBook")
    protected MatchGuideBook guideBook;

    @Inject
    private MatchActorSystem matchActorSystem;

    @Inject
    private EntityMatchConfigurationService entityMatchConfigurationService;

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

    @Override
    protected boolean shouldPrepareRetravel(Traveler traveler) {
        String entity = (traveler instanceof MatchTraveler) ? ((MatchTraveler) traveler).getEntity() : null;
        if (!entityMatchConfigurationService.isAllocateMode(entity)) {
            return false;
        }
        return super.shouldPrepareRetravel(traveler);
    }

}
