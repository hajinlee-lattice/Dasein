package com.latticeengines.actors.visitor.sample;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.latticeengines.actors.exposed.ActorSystemTemplate;
import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.actors.template.VisitorActorTemplate;
import com.latticeengines.actors.utils.ActorUtils;
import com.latticeengines.actors.visitor.sample.framework.SampleMatchActorSystem;
import com.latticeengines.actors.visitor.sample.framework.SampleMatchGuideBook;

import akka.actor.ActorRef;

public abstract class SampleMicroEngineActorTemplate<T extends SampleDataSourceWrapperActorTemplate>
        extends VisitorActorTemplate {

    protected abstract Class<T> getDataSourceActorClz();

    @Override
    protected abstract boolean accept(Traveler traveler);

    @Override
    protected abstract void process(Response response);

    @Autowired
    private SampleMatchActorSystem matchActorSystem;

    @Autowired
    @Qualifier("sampleMatchGuideBook")
    protected SampleMatchGuideBook guideBook;

    @Override
    public GuideBook getGuideBook() {
        return guideBook;
    }

    @Override
    protected ActorSystemTemplate getActorSystem() {
        return matchActorSystem;
    }

    @Override
    protected boolean needAssistantActor() {
        return true;
    }

    @Override
    protected boolean isValidMessageType(Object msg) {
        return msg instanceof SampleMatchTravelContext || msg instanceof Response;
    }

    @Override
    protected boolean process(Traveler context) {
        SampleMatchTravelContext travelContext = (SampleMatchTravelContext) context;
        if (accept(travelContext)) {
            ActorRef nextActorRef = matchActorSystem.getActorRef(getDataSourceActorClz());

            SampleDataSourceLookupRequest req = new SampleDataSourceLookupRequest();
            req.setMatchTravelerContext(travelContext);
            req.setInputData(travelContext.getMatchKeyTuple());
            guideBook.logVisit(ActorUtils.getPath(self()), travelContext);

            nextActorRef.tell(req, self());
            return true;
        } else {
            guideBook.logVisit(ActorUtils.getPath(self()), travelContext);
            return false;
        }
    }

    @Override
    protected String getNextLocation(Traveler traveler) {
        return guideBook.next(ActorUtils.getPath(self()), traveler);
    }
}
