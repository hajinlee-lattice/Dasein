package com.latticeengines.datacloud.match.actors.visitor;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;

import com.latticeengines.actors.exposed.ActorSystemTemplate;
import com.latticeengines.actors.exposed.traveler.GuideBook;
import com.latticeengines.actors.exposed.traveler.Response;
import com.latticeengines.actors.exposed.traveler.Traveler;
import com.latticeengines.actors.template.ProxyMicroEngineTemplate;
import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.framework.MatchGuideBook;
import com.latticeengines.datacloud.match.service.MatchMetricService;
import com.latticeengines.domain.exposed.actors.VisitingHistory;
import com.latticeengines.domain.exposed.datacloud.match.MatchKeyTuple;

import akka.actor.ActorRef;

/**
 * Template for micro-engines in the match which does data source lookup
 *
 * @param <T>
 */
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

    /**
     * Record the LDC Match actor and tuple for Match Report.
     */
    protected abstract void recordActorAndTuple(MatchTraveler traveler);


    @Resource(name = "matchActorSystem")
    protected MatchActorSystem matchActorSystem;

    @Resource(name = "matchGuideBook")
    protected MatchGuideBook guideBook;

    @Inject
    @Lazy
    private MatchMetricService metricService;

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
        if (accept(matchTraveler)) {
            recordActorAndTuple(matchTraveler);
            return true;
        } else {
            return false;
        }
    }

    @Override
    protected void sendReqToAssistantActor(Traveler traveler) {
        MatchTraveler matchTraveler = (MatchTraveler) traveler;
        ActorRef datasourceRef = matchActorSystem.getActorRef(getDataSourceActorClz());
        DataSourceLookupRequest req = new DataSourceLookupRequest();
        req.setMatchTravelerContext(matchTraveler);
        // try to generate input data with legacy method first (using match key tuple)
        Object inputData = prepareInputData(matchTraveler.getMatchKeyTuple());
        if (inputData == null) {
            // try new method with more flexible input data type
            inputData = prepareInputData(matchTraveler);
        }
        req.setInputData(inputData);
        datasourceRef.tell(req, self());
    }

    @Override
    protected void writeVisitingHistory(VisitingHistory history) {
        try {
            history.setActorSystemMode(matchActorSystem.isBatchMode() ? "Batch" : "Realtime");
            metricService.recordActorVisit(history);
        } catch (Exception e) {
            log.warn("Failed to extract output metric.");
        }
    }

    /*
     * Legacy method that takes a tuple and returns a tuple.
     */
    @Deprecated
    protected MatchKeyTuple prepareInputData(MatchKeyTuple input) {
        return input;
    }

    /*
     * Method to prepare input data for DataSourceLookupService
     */
    protected Object prepareInputData(MatchTraveler traveler) {
        return null;
    }
}
