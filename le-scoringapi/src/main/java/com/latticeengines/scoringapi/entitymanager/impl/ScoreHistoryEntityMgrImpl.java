package com.latticeengines.scoringapi.entitymanager.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datafabric.entitymanager.GenericFabricEntityManager;
import com.latticeengines.domain.exposed.datafabric.FabricStoreEnum;
import com.latticeengines.domain.exposed.datafabric.generic.GenericRecordRequest;
import com.latticeengines.domain.exposed.scoringapi.Record;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.ScoreRecordHistory;
import com.latticeengines.domain.exposed.scoringapi.ScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.ScoreResponse;
import com.latticeengines.scoringapi.entitymanager.ScoreHistoryEntityMgr;

@Component("scoreHistoryEntityMgr")
public class ScoreHistoryEntityMgrImpl implements ScoreHistoryEntityMgr {

    private static final Log log = LogFactory.getLog(ScoreHistoryEntityMgrImpl.class);

    private static final String FABRIC_SCORE_HISTORY = "FabricScoreHistory";

    @Resource(name = "genericFabricEntityManager")
    private GenericFabricEntityManager<ScoreRecordHistory> fabricEntityManager;

    @Value("${scoringapi.score.history.publish.enabled:false}")
    private boolean shouldPublish;

    @PostConstruct
    public void init() {
        if (shouldPublish) {
            fabricEntityManager.createOrGetNamedBatchId(FABRIC_SCORE_HISTORY, null, false);
        }
    }

    @Override
    public void publish(String tenantId, List<Record> requests, List<RecordScoreResponse> responses) {

        Map<String, Record> requestMap = new HashMap<String, Record>();

        for (Record request : requests)
            requestMap.put(request.getRecordId(), request);

        for (RecordScoreResponse response : responses) {
            ScoreRecordHistory scoreHistory = buildScoreHistory(tenantId, requestMap.get(response.getId()), response);
            log.debug("Publish history id " + scoreHistory.getId() + "record " + scoreHistory.getIdType() + " "
                    + scoreHistory.getRecordId() + " latticeId " + scoreHistory.getLatticeId());
            publishScoreHistory(scoreHistory);
        }
    }

    private void publishScoreHistory(ScoreRecordHistory scoreHistory) {
        GenericRecordRequest recordRequest = getGenericRequest();
        recordRequest.setId(scoreHistory.getId());
        fabricEntityManager.publishEntity(recordRequest, scoreHistory, ScoreRecordHistory.class);
    }

    private GenericRecordRequest getGenericRequest() {
        GenericRecordRequest recordRequest = new GenericRecordRequest();
        recordRequest.setStores(Arrays.asList(FabricStoreEnum.HDFS))
                .setRepositories(Arrays.asList(FABRIC_SCORE_HISTORY)).setBatchId(FABRIC_SCORE_HISTORY);
        return recordRequest;
    }

    @Override
    public void publish(String tenantId, Record request, RecordScoreResponse response) {

        // Ignore score requests without lattice Id;
        if (response.getLatticeId() == null)
            return;
        ScoreRecordHistory scoreHistory = buildScoreHistory(tenantId, request, response);
        log.debug("Publish history id " + scoreHistory.getId() + "record " + scoreHistory.getIdType() + " "
                + scoreHistory.getRecordId() + " latticeId " + scoreHistory.getLatticeId());
        publishScoreHistory(scoreHistory);
    }

    @Override
    public void publish(String tenantId, ScoreRequest request, ScoreResponse response) {
        // Ignore score requests without lattice Id;
        if (response.getLatticeId() == null)
            return;

        Record record = new Record();
        record.setRecordId(response.getId());
        record.setIdType("Unknown");
        record.setPerformEnrichment(false);
        record.setRootOperationId("");
        record.setRule(request.getRule());
        record.setRequestTimestamp(response.getTimestamp());
        Map<String, Map<String, Object>> attrMap = new HashMap<String, Map<String, Object>>();
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

        ScoreRecordHistory scoreHistory = buildScoreHistory(tenantId, record, recordRsp);

        log.debug("Publish history id " + scoreHistory.getId() + "record " + scoreHistory.getIdType() + " "
                + scoreHistory.getRecordId() + " latticeId " + scoreHistory.getLatticeId());
        publishScoreHistory(scoreHistory);
    }

    private ScoreRecordHistory buildScoreHistory(String tenantId, Record request, RecordScoreResponse response) {

        ScoreRecordHistory history = new ScoreRecordHistory();

        String id = constructHistoryId(request.getIdType(), request.getRecordId(), request.getRequestTimestamp());

        history.setId(id);
        history.setRequestTimestamp(request.getRequestTimestamp());
        history.setLatticeId(response.getLatticeId());
        history.setIdType(request.getIdType());
        history.setRecordId(response.getId());
        history.setLatticeId(response.getLatticeId());
        history.setTenantId(tenantId);
        history.setRequest(JsonUtils.serialize(request));
        history.setResponse(JsonUtils.serialize(response));
        return history;
    }

    private String constructHistoryId(String recordType, String recordId, String timestamp) {
        return recordType + "_" + recordId + "_" + timestamp;
    }
}
