package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.datacloud.match.service.FuzzyMatchService;
import com.latticeengines.domain.exposed.datacloud.match.NameLocation;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;

import akka.pattern.Patterns;
import akka.util.Timeout;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

@Component
public class FuzzyMatchServiceImpl implements FuzzyMatchService {
    private static final Log log = LogFactory.getLog(FuzzyMatchServiceImpl.class);

    private static final Timeout REALTIME_TIMEOUT =new Timeout(new FiniteDuration(10, TimeUnit.MINUTES));
    private static final Timeout BATCH_TIMEOUT = new Timeout(new FiniteDuration(1, TimeUnit.HOURS));

    @Autowired
    private MatchActorSystem actorSystem;

    @Override
    public void callMatch(List<OutputRecord> matchRecords, String rootOperationUid, String dataCloudVersion)
            throws Exception {
        checkRecordType(matchRecords);

        Timeout timeout = actorSystem.isBatchMode() ? BATCH_TIMEOUT : REALTIME_TIMEOUT;
        List<Future<Object>> matchFutures = new ArrayList<>();

        for (OutputRecord record : matchRecords) {
            InternalOutputRecord matchRecord = (InternalOutputRecord) record;
            MatchTraveler travelContext = new MatchTraveler(rootOperationUid);
            matchRecord.setTravelerId(travelContext.getTravelerId());

            MatchKeyTuple matchKeyTuple = createMatchKeyTuple(matchRecord);
            travelContext.setMatchKeyTuple(matchKeyTuple);
            travelContext.setDataCloudVersion(dataCloudVersion);

            matchFutures.add(askFuzzyMatchAnchor(travelContext, timeout));
        }

        int idx = 0;
        for (Future<Object> future : matchFutures) {
            MatchTraveler traveler = (MatchTraveler) Await.result(future, timeout.duration());
            log.info("Got LatticeAccountId=" + traveler.getResult() + " for TravelerId=" + traveler.getTravelerId()
                    + " in RootOperationUID=" + traveler.getRootOperationUid());
            InternalOutputRecord matchRecord = (InternalOutputRecord) matchRecords.get(idx++);
            if (traveler.getResult() != null) {
                matchRecord.setLatticeAccountId((String) traveler.getResult());
            }
        }
    }

    private Future<Object> askFuzzyMatchAnchor(MatchTraveler traveler, Timeout timeout) {
        return Patterns.ask(actorSystem.getFuzzyMatchAnchor(), traveler, timeout);
    }

    private void checkRecordType(List<OutputRecord> matchRequests) {
        if (matchRequests.size() > actorSystem.getMaxAllowedRecordCount()) {
            throw new RuntimeException("Too many records in the request: " + matchRequests.size()
                    + ", max allowed record count = " + actorSystem.getMaxAllowedRecordCount());
        }
        for (OutputRecord matchRequest : matchRequests) {
            if (!(matchRequest instanceof InternalOutputRecord)) {
                throw new RuntimeException("Expected request of type " + InternalOutputRecord.class);
            }
        }
    }

    private MatchKeyTuple createMatchKeyTuple(InternalOutputRecord matchRecord) {
        MatchKeyTuple matchKeyTuple = new MatchKeyTuple();
        NameLocation nameLocationInfo = matchRecord.getParsedNameLocation();
        if (nameLocationInfo != null) {
            matchKeyTuple.setCity(nameLocationInfo.getCity());
            matchKeyTuple.setCountry(nameLocationInfo.getCountry());
            matchKeyTuple.setName(nameLocationInfo.getName());
            matchKeyTuple.setState(nameLocationInfo.getState());
            matchKeyTuple.setZipcode(nameLocationInfo.getZipCode());
            matchKeyTuple.setPhoneNumber(nameLocationInfo.getPhoneNumber());
        }
        matchKeyTuple.setDomain(matchRecord.getParsedDomain());
        matchKeyTuple.setDuns(matchRecord.getParsedDuns());
        return matchKeyTuple;
    }
}
