package com.latticeengines.datacloud.match.actors.visitor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.actors.visitor.VisitorActorTemplate;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.framework.MatchGuideBook;

import akka.actor.ActorRef;

public abstract class MicroEngineActorTemplate<T extends DataSourceWrapperActorTemplate> extends VisitorActorTemplate {

    protected abstract Class<T> getDataSourceActorClz();

    protected abstract boolean accept(Traveler traveler);

    protected abstract void process(Response response);

    @Autowired
    @Qualifier("matchActorSystem")
    protected MatchActorSystem matchActorSystem;

    @Autowired
    @Qualifier("matchGuideBook")
    protected MatchGuideBook guideBook;

    @Override
    public GuideBook getGuideBook() {
        return guideBook;
    }

    @Override
    protected boolean isValidMessageType(Object msg) {
        return msg instanceof MatchTraveler || msg instanceof Response;
    }

    @Override
    protected boolean process(Traveler context) {
        MatchTraveler matchTraveler = (MatchTraveler) context;
        if (accept(matchTraveler)) {
            ActorRef datasourceRef = matchActorSystem.getActorRef(getDataSourceActorClz());

            DataSourceLookupRequest req = new DataSourceLookupRequest();
            req.setMatchTravelerContext(matchTraveler);
            req.setInputData(prepareInputData(matchTraveler.getMatchKeyTuple()));
            guideBook.logVisit(self().path().toSerializationFormat(), matchTraveler);

            datasourceRef.tell(req, self());
            return true;
        } else {
            matchTraveler.debug("Rejected by " + getClass().getSimpleName());
            guideBook.logVisit(self().path().toSerializationFormat(), matchTraveler);
            return false;
        }
    }

    @Override
    protected String getNextLocation(Traveler traveler) {
        return guideBook.next(getSelf().path().toSerializationFormat(), traveler);
    }

    @Override
    protected String getActorName(ActorRef actorRef) {
        return matchActorSystem.getActorName(actorRef);
    }

    protected MatchKeyTuple prepareInputData(MatchKeyTuple input) {
        return input;
    }
}
