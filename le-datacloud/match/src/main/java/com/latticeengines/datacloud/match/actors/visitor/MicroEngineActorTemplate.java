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
    private MatchActorSystem matchActorSystem;

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
        MatchTraveler travelContext = (MatchTraveler) context;
        if (accept(travelContext)) {
            ActorRef datasourceRef = matchActorSystem.getActorRef(getDataSourceActorClz());

            DataSourceLookupRequest req = new DataSourceLookupRequest();
            req.setMatchTravelerContext(travelContext);
            req.setInputData(travelContext.getMatchKeyTuple());
            guideBook.logVisit(self().path().toSerializationFormat(), travelContext);

            datasourceRef.tell(req, self());
            return true;
        } else {
            guideBook.logVisit(self().path().toSerializationFormat(), travelContext);
            return false;
        }
    }

    @Override
    protected String getNextLocation(Traveler traveler) {
        return guideBook.next(getSelf().path().toSerializationFormat(), traveler);
    }
}
