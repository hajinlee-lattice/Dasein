package com.latticeengines.datacloud.match.actors.visitor;

import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.TravelContext;
import com.latticeengines.actors.visitor.VisitorActorTemplate;

import akka.actor.ActorRef;

public abstract class MicroEngineActorTemplate extends VisitorActorTemplate {
    protected abstract String getDataSourceActor();

    protected abstract boolean accept(TravelContext traveler);

    @Override
    protected boolean isValidMessageType(Object msg) {
        return msg instanceof MatchTravelContext || msg instanceof Response;
    }

    @Override
    protected boolean process(TravelContext traveler) {
        if (accept(traveler)) {
            ActorRef nextActorRef = ((MatchGuideBook) guideBook).getDataSourceActorRef(getDataSourceActor());

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
        GuideBook guideBook = traveler.getGuideBook();
        String nextLocation = guideBook.next(getSelf().path().toSerializationFormat(), traveler);
        return nextLocation;
    }
}
