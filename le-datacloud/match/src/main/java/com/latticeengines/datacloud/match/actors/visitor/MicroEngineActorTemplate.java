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

    @Autowired
    private MatchActorSystem matchActorSystem;

    @Autowired
    private MatchGuideBook guideBook;

    @Override
    protected boolean isValidMessageType(Object msg) {
        return msg instanceof MatchTravelContext || msg instanceof Response;
    }

    @Override
    protected boolean process(TravelContext traveler) {
        if (accept(traveler)) {
            ActorRef nextActorRef =  matchActorSystem.getActorRef(getDataSourceActorClz());

            DataSourceLookupRequest req = new DataSourceLookupRequest();
            req.setMatchTravelerContext((MatchTravelContext) traveler);
            req.setInputData(traveler.getDataKeyValueMap());
            guideBook.logVisit(self().path().toSerializationFormat(), traveler);

            nextActorRef.tell(req, self());
            return true;
        } else {
            traveler.logVisit(self().path().toSerializationFormat());
            return false;
        }
    }

    @Override
    protected void process(Response response) {
        // may be do something
    }

    @Override
    protected String getNextLocation(TravelContext traveler) {
        return guideBook.next(getSelf().path().toSerializationFormat(), traveler);
    }
}
