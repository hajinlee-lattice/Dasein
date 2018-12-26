package com.latticeengines.datacloud.match.actors.visitor;

import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import com.latticeengines.actors.exposed.ActorSystemTemplate;
import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.actors.template.ProxyMicroEngineTemplate;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.framework.MatchGuideBook;
import com.latticeengines.domain.exposed.actors.MeasurementMessage;
import com.latticeengines.domain.exposed.actors.VisitingHistory;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;

import akka.actor.ActorRef;

public abstract class DataSourceMicroEngineTemplate<T extends DataSourceWrapperActorTemplate>
        extends ProxyMicroEngineTemplate {

    private static final Logger log = LoggerFactory.getLogger(DataSourceMicroEngineTemplate.class);

    /**
     * @return class of data source actor
     */
    protected abstract Class<T> getDataSourceActorClz();

    /**
     * Whether match traveler is accepted
     * 
     * @param traveler
     * @return
     */
    protected abstract boolean accept(MatchTraveler traveler);

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
    protected ActorSystemTemplate getActorSystem() {
        return matchActorSystem;
    }

    @Override
    protected boolean isValidMessageType(Object msg) {
        return msg instanceof MatchTraveler || msg instanceof Response;
    }

    @Override
    protected boolean accept(Traveler traveler) {
        MatchTraveler matchTraveler = (MatchTraveler) traveler;
        return accept(matchTraveler);
    }

    @Override
    protected void sendReqToAssistantActor(Traveler traveler) {
        MatchTraveler matchTraveler = (MatchTraveler) traveler;
        ActorRef datasourceRef = matchActorSystem.getActorRef(getDataSourceActorClz());
        DataSourceLookupRequest req = new DataSourceLookupRequest();
        req.setMatchTravelerContext(matchTraveler);
        req.setInputData(prepareInputData(matchTraveler.getMatchKeyTuple()));
        datasourceRef.tell(req, self());
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
