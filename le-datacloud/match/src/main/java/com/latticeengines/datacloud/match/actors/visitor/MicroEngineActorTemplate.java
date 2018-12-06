package com.latticeengines.datacloud.match.actors.visitor;

import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.actors.utils.ActorUtils;
import com.latticeengines.actors.visitor.VisitorActorTemplate;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.framework.MatchGuideBook;
import com.latticeengines.domain.exposed.actors.MeasurementMessage;
import com.latticeengines.domain.exposed.actors.VisitingHistory;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;

import akka.actor.ActorRef;

public abstract class MicroEngineActorTemplate<T extends DataSourceWrapperActorTemplate> extends VisitorActorTemplate {

    private static final Logger log = LoggerFactory.getLogger(MicroEngineActorTemplate.class);

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
            guideBook.logVisit(ActorUtils.getPath(self()), matchTraveler);

            datasourceRef.tell(req, self());
            return true;
        } else {
            matchTraveler.debug("Rejected by " + getClass().getSimpleName());
            guideBook.logVisit(ActorUtils.getPath(self()), matchTraveler);
            return false;
        }
    }

    @Override
    protected String getNextLocation(Traveler traveler) {
        return guideBook.next(ActorUtils.getPath(getSelf()), traveler);
    }

    @Override
    protected String getActorName(ActorRef actorRef) {
        return matchActorSystem.getActorName(actorRef);
    }

    @Override
    protected void writeVisitingHistory(VisitingHistory history) {
        try {
            history.setActorSystemMode(matchActorSystem.isBatchMode() ? "Batch" : "Realtime");
            MeasurementMessage<VisitingHistory> message = new MeasurementMessage<>();
            message.setMeasurements(Collections.singletonList(history));
            message.setMetricDB(MetricDB.LDC_Match);
            matchActorSystem.getMetricActor().tell(message, null);
        } catch (Exception e) {
            log.warn("Failed to extract output metric.");
        }
    }

    protected MatchKeyTuple prepareInputData(MatchKeyTuple input) {
        return input;
    }
}
