package com.latticeengines.datacloud.match.actors.visitor;

import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.TravelerContext;
import com.latticeengines.actors.visitor.VisitorActorTemplate;

import akka.actor.ActorRef;

public abstract class MicroEngineActorTemplate extends VisitorActorTemplate {
    protected abstract String getDataSourceActor();

    protected abstract boolean accept(TravelerContext traveler);

    @Override
    protected boolean isValidMessageType(Object msg) {
        return msg instanceof MatchTravelerContext || msg instanceof Response;
    }

    @Override
    protected boolean process(TravelerContext traveler) {
        if (accept(traveler)) {
            String dataSourceActor = getDataSourceActor();
            dataSourceActor = traveler.getGuideBook().getDataSourceActorPath(dataSourceActor);
            ActorRef nextActorRef = getContext().actorFor(dataSourceActor);

            DataSourceLookupRequest req = new DataSourceLookupRequest();
            req.setMatchTravelerContext((MatchTravelerContext) traveler);
            req.setInputData(traveler.getDataKeyValueMap());
            traveler.updateVisitedHistoryInfo(self().path().toSerializationFormat());

            nextActorRef.tell(req, self());
            return true;
        } else {
            traveler.updateVisitedHistoryInfo(self().path().toSerializationFormat());
            return false;
        }
    }

    @Override
    protected void process(Response response) {
        // may be do something
    }

    @Override
    protected String getNextLocation(TravelerContext traveler) {
        GuideBook guideBook = traveler.getGuideBook();
        String nextLocation = guideBook.next(getSelf().path().toSerializationFormat(), traveler);
        return nextLocation;
    }
}
