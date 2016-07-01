package com.latticeengines.scoringapi.entitymanager.impl;

import com.latticeengines.scoringapi.entitymanager.ScoreHistoryEntityMgr;
import com.latticeengines.domain.exposed.scoringapi.Record;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.ScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.ScoreRecordHistory;
import com.latticeengines.domain.exposed.scoringapi.ScoreResponse;
import com.latticeengines.datafabric.entitymanager.impl.BaseFabricEntityMgrImpl;
import com.latticeengines.common.exposed.util.JsonUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.springframework.stereotype.Component;

@Component("scoreHistoryEntityMgr")
public class ScoreHistoryEntityMgrImpl extends BaseFabricEntityMgrImpl<ScoreRecordHistory> implements ScoreHistoryEntityMgr {

    private static final Log log = LogFactory.getLog(ScoreHistoryEntityMgrImpl.class);

    public ScoreHistoryEntityMgrImpl() {
        super(new BaseFabricEntityMgrImpl.Builder().
                  recordType("ScoreHistory").
                  topic("ScoreHistory").
                  store("REDIS").
                  repository("RTS"));
    }

    public void publish(List<Record> requests, List<RecordScoreResponse> responses) {

        if (isDisabled()) return;

        Map<String, Record> requestMap = new HashMap<String, Record>();

        for (Record request : requests)
             requestMap.put(request.getRecordId(), request);

        for (RecordScoreResponse response : responses) {
            ScoreRecordHistory scoreHistory = buildScoreHistory(requestMap.get(response.getId()), response);
            log.debug("Publish history id " + scoreHistory.getId() + "record " + scoreHistory.getIdType() + " "+ scoreHistory.getRecordId() +
                     " latticeId " +scoreHistory.getLatticeId());
            super.publish(scoreHistory);
        }
    }

    public void publish(Record request, RecordScoreResponse response) {

        if (isDisabled()) return;
        // Ignore score requests without lattice Id;
        if (response.getLatticeId() == null) return;
        ScoreRecordHistory scoreHistory = buildScoreHistory(request, response);
        log.debug("Publish history id " + scoreHistory.getId() + "record " + scoreHistory.getIdType() + " "+ scoreHistory.getRecordId() +
                 " latticeId " +scoreHistory.getLatticeId());
        super.publish(scoreHistory);
    }

    public void publish(ScoreRequest request, ScoreResponse response) {
        if (isDisabled()) return;
        // Ignore score requests without lattice Id;
        if (response.getLatticeId() == null) return;

        Record record = new Record();
        record.setRecordId(response.getId());
        record.setIdType("Unknown");
        record.setPerformEnrichment(false);
        record.setRootOperationId("");
        record.setRule(request.getRule());
        record.setRequestTimestamp(response.getTimestamp());
        Map<String, Map<String, Object>>  attrMap = new HashMap<String, Map<String, Object>>();
        attrMap.put(request.getModelId(), request.getRecord());
        record.setModelAttributeValuesMap(attrMap);

        RecordScoreResponse recordRsp = new RecordScoreResponse();
        recordRsp.setId(response.getId());
        recordRsp.setLatticeId(response.getLatticeId());
        List<RecordScoreResponse.ScoreModelTuple> scores = new ArrayList<RecordScoreResponse.ScoreModelTuple>();
        RecordScoreResponse.ScoreModelTuple tuple = new RecordScoreResponse.ScoreModelTuple();
        tuple.setModelId(request.getModelId());
        tuple.setScore(response.getScore());
        tuple.setError("");
        tuple.setErrorDescription("");
        scores.add(tuple);
        recordRsp.setScores(scores);
        recordRsp.setEnrichmentAttributeValues(response.getEnrichmentAttributeValues());
        recordRsp.setWarnings(response.getWarnings());
        recordRsp.setTimestamp(response.getTimestamp());

        ScoreRecordHistory scoreHistory = buildScoreHistory(record, recordRsp);

        log.debug("Publish history id " + scoreHistory.getId() + "record " + scoreHistory.getIdType() + " "+ scoreHistory.getRecordId() +
                 " latticeId " +scoreHistory.getLatticeId());
        super.publish(scoreHistory);
    }

    public List<ScoreRecordHistory> findByLatticeId(String latticeId) {
        if (isDisabled()) return null;

        Map<String, String> properties = new HashMap<String, String>();
        properties.put("latticeId", latticeId);
        List<ScoreRecordHistory> recordHistories = super.findByProperties(properties);
        return recordHistories;
    }

    private ScoreRecordHistory buildScoreHistory(Record request, RecordScoreResponse response) {

        ScoreRecordHistory history = new ScoreRecordHistory();

        String id = constructHistoryId(request.getIdType(), request.getRecordId(), request.getRequestTimestamp());

        history.setId(id);
        history.setRequestTimestamp(request.getRequestTimestamp());
        history.setLatticeId(response.getLatticeId());
        history.setIdType(request.getIdType());
        history.setRecordId(response.getId());
        history.setLatticeId(response.getLatticeId());
        history.setRequest(JsonUtils.serialize(request));
        history.setResponse(JsonUtils.serialize(response));
        return history;
    }


    private String constructHistoryId(String recordType, String recordId, String timestamp) {
        return recordType + "_" + recordId + "_" + timestamp;
    }
}
