package com.latticeengines.datacloud.match.actors.visitor;

import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.TravelContext;
import com.latticeengines.actors.visitor.VisitorActorTemplate;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.framework.MatchGuideBook;

import akka.actor.ActorRef;

public abstract class MicroEngineActorTemplate<T extends DataSourceWrapperActorTemplate> extends VisitorActorTemplate {

    protected abstract Class<T> getDataSourceActorClz();

    protected abstract boolean accept(TravelContext traveler);

    protected abstract void process(Response response);

    @Autowired
    private MatchActorSystem matchActorSystem;

    @Autowired
    private MatchGuideBook guideBook;

    @Override
    protected boolean isValidMessageType(Object msg) {
        return msg instanceof MatchTravelContext || msg instanceof Response;
    }

    @Override
    protected boolean process(TravelContext context) {
        MatchTravelContext travelContext = (MatchTravelContext) context;
        if (accept(travelContext)) {
            ActorRef nextActorRef = matchActorSystem.getActorRef(getDataSourceActorClz());

            DataSourceLookupRequest req = new DataSourceLookupRequest();
            req.setMatchTravelerContext(travelContext);
            req.setInputData(travelContext.getMatchKeyTuple());
            guideBook.logVisit(self().path().toSerializationFormat(), travelContext);

            nextActorRef.tell(req, self());
            return true;
        } else {
            guideBook.logVisit(self().path().toSerializationFormat(), travelContext);
            return false;
        }
    }

    @Override
    protected String getNextLocation(TravelContext traveler) {
        return guideBook.next(getSelf().path().toSerializationFormat(), traveler);
    }
}
