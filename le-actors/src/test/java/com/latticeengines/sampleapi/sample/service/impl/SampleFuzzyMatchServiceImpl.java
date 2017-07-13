package com.latticeengines.sampleapi.sample.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.actors.visitor.sample.SampleMatchKeyTuple;
import com.latticeengines.actors.visitor.sample.SampleMatchTravelContext;
import com.latticeengines.actors.visitor.sample.framework.SampleMatchActorSystem;
import com.latticeengines.sampleapi.sample.service.SampleFuzzyMatchService;
import com.latticeengines.sampleapi.sample.service.SampleInternalOutputRecord;
import com.latticeengines.sampleapi.sample.service.SampleNameLocation;
import com.latticeengines.sampleapi.sample.service.SampleOutputRecord;

import akka.pattern.Patterns;
import akka.util.Timeout;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.FiniteDuration;

@Component
public class SampleFuzzyMatchServiceImpl implements SampleFuzzyMatchService {
    private static final Logger log = LoggerFactory.getLogger(SampleFuzzyMatchServiceImpl.class);

    private static final int MAX_ALLOWED_RECORD_COUNT = 200;

    @Autowired
    private SampleMatchActorSystem actorSystem;

    @Override
    public void callMatch(SampleOutputRecord matchRecord, String dataCloudVersion) throws Exception {
        checkRecordType(matchRecord);
        List<SampleOutputRecord> matchRecords = new ArrayList<>();
        matchRecords.add(matchRecord);
        callMatch(matchRecords, dataCloudVersion);
    }

    @Override
    public void callMatch(List<SampleOutputRecord> matchRecords, String dataCloudVersion) throws Exception {
        checkRecordType(matchRecords);

        FiniteDuration duration = new FiniteDuration(10, TimeUnit.MINUTES);
        Timeout timeout = new Timeout(duration);
        List<Future<Object>> matchFutures = new ArrayList<>();

        for (SampleOutputRecord record : matchRecords) {
            SampleInternalOutputRecord matchRecord = (SampleInternalOutputRecord) record;
            SampleMatchTravelContext travelContext = //
                    new SampleMatchTravelContext(UUID.randomUUID().toString());

            SampleMatchKeyTuple matchKeyTuple = createMatchKeyTuple(matchRecord);
            travelContext.setMatchKeyTuple(matchKeyTuple);
            travelContext.setDataCloudVersion(dataCloudVersion);

            matchFutures.add(askFuzzyMatchAnchor(travelContext, timeout));
        }

        int idx = 0;
        for (Future<Object> future : matchFutures) {
            SampleMatchTravelContext result = (SampleMatchTravelContext) Await.result(future, timeout.duration());
            log.info("Got result: " + result);
            SampleInternalOutputRecord matchRecord = (SampleInternalOutputRecord) matchRecords.get(idx++);
            matchRecord.setLatticeAccountId((String) result.getResult());
        }
    }

    private Future<Object> askFuzzyMatchAnchor(SampleMatchTravelContext traveler, Timeout timeout) {
        return Patterns.ask(actorSystem.getFuzzyMatchAnchor(), traveler, timeout);
    }

    private void checkRecordType(SampleOutputRecord matchRequest) {
        List<SampleOutputRecord> matchRequests = new ArrayList<>();
        matchRequests.add(matchRequest);
        checkRecordType(matchRequests);
    }

    private void checkRecordType(List<SampleOutputRecord> matchRequests) {
        if (matchRequests.size() > MAX_ALLOWED_RECORD_COUNT) {
            throw new RuntimeException("Too many records in the request: " + matchRequests.size()
                    + ", max allowed record count = " + MAX_ALLOWED_RECORD_COUNT);
        }
        for (SampleOutputRecord matchRequest : matchRequests) {
            if (!(matchRequest instanceof SampleInternalOutputRecord)) {
                throw new RuntimeException("Expected request of type " + SampleInternalOutputRecord.class);
            }
        }
    }

    private SampleMatchKeyTuple createMatchKeyTuple(SampleInternalOutputRecord matchRecord) {
        SampleMatchKeyTuple matchKeyTuple = new SampleMatchKeyTuple();
        SampleNameLocation nameLocationInfo = matchRecord.getParsedNameLocation();
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
