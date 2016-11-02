package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.actors.framework.MatchActorSystem;
import com.latticeengines.datacloud.match.actors.visitor.MatchKeyTuple;
import com.latticeengines.datacloud.match.actors.visitor.MatchTravelContext;
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

    private static final int MAX_ALLOWED_RECORD_COUNT = 200;

    @Autowired
    private MatchActorSystem actorSystem;

    @Override
    public void callMatch(OutputRecord matchRecord, String dataCloudVersion) throws Exception {
        checkRecordType(matchRecord);
        List<OutputRecord> matchRecords = new ArrayList<>();
        matchRecords.add(matchRecord);
        callMatch(matchRecords, dataCloudVersion);
    }

    @Override
    public void callMatch(List<OutputRecord> matchRecords, String dataCloudVersion) throws Exception {
        checkRecordType(matchRecords);

        FiniteDuration duration = new FiniteDuration(10, TimeUnit.MINUTES);
        Timeout timeout = new Timeout(duration);
        List<Future<Object>> matchFutures = new ArrayList<>();

        for (OutputRecord record : matchRecords) {
            InternalOutputRecord matchRecord = (InternalOutputRecord) record;
            MatchTravelContext travelContext = //
                    new MatchTravelContext(UUID.randomUUID().toString());

            MatchKeyTuple matchKeyTuple = createMatchKeyTuple(matchRecord);
            travelContext.setMatchKeyTuple(matchKeyTuple);
            travelContext.setDataCloudVersion(dataCloudVersion);

            matchFutures.add(askFuzzyMatchAnchor(travelContext, timeout));
        }

        int idx = 0;
        for (Future<Object> future : matchFutures) {
            String result = (String) Await.result(future, timeout.duration());
            log.info("Got result: " + result);
            InternalOutputRecord matchRecord = (InternalOutputRecord) matchRecords.get(idx++);
            matchRecord.setLatticeAccountId(result);
        }
    }

    private Future<Object> askFuzzyMatchAnchor(MatchTravelContext traveler, Timeout timeout) {
        return Patterns.ask(actorSystem.getFuzzyMatchAnchor(), traveler, timeout);
    }

    private void checkRecordType(OutputRecord matchRequest) {
        List<OutputRecord> matchRequests = new ArrayList<>();
        matchRequests.add(matchRequest);
        checkRecordType(matchRequests);
    }

    private void checkRecordType(List<OutputRecord> matchRequests) {
        if (matchRequests.size() > MAX_ALLOWED_RECORD_COUNT) {
            throw new RuntimeException("Too many records in the request: " + matchRequests.size()
                    + ", max allowed record count = " + MAX_ALLOWED_RECORD_COUNT);
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
        }

        matchKeyTuple.setDomain(matchRecord.getParsedDomain());
        matchKeyTuple.setDuns(matchRecord.getParsedDuns());
        return matchKeyTuple;
    }
}
