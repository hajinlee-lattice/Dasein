package com.latticeengines.scoringapi.entitymanager.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.aws.firehose.FirehoseService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.scoringapi.Record;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.ScoreRecordHistory;
import com.latticeengines.domain.exposed.scoringapi.ScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.ScoreResponse;
import com.latticeengines.scoringapi.entitymanager.ScoreHistoryEntityMgr;

@Component("scoreHistoryEntityMgr")
public class ScoreHistoryEntityMgrImpl implements ScoreHistoryEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(ScoreHistoryEntityMgrImpl.class);

    @Value("${scoringapi.score.history.publish.enabled:false}")
    private boolean shouldPublish;

    @Value("${scoringapi.score.history.delivery.stream.name}")
    private String deliveryStreamName;

    @Inject
    private FirehoseService firehoseService;

    protected DateTimeFormatter timestampFormatter = ISODateTimeFormat.dateTime();

    @PostConstruct
    public void init() {
    }

    @Override
    public void publish(String tenantId, List<Record> requests, List<RecordScoreResponse> responses) {

        Map<String, Record> requestMap = new HashMap<String, Record>();

        for (Record request : requests)
            requestMap.put(request.getRecordId(), request);

        List<String> histories = new ArrayList<>();
        for (RecordScoreResponse response : responses) {
            ScoreRecordHistory scoreHistory = buildScoreHistory(tenantId, requestMap.get(response.getId()), response);
            log.debug("Publish history id " + scoreHistory.getId() + "record " + scoreHistory.getIdType() + " "
                    + scoreHistory.getRecordId() + " latticeId " + scoreHistory.getLatticeId());
            histories.add(JsonUtils.serialize(scoreHistory));
        }
        firehoseService.sendBatch(deliveryStreamName, null, histories);
    }

    private void publishScoreHistory(ScoreRecordHistory scoreHistory) {
        firehoseService.send(deliveryStreamName, null, JsonUtils.serialize(scoreHistory));
    }

    @Override
    public void publish(String tenantId, Record request, RecordScoreResponse response) {
        ScoreRecordHistory scoreHistory = buildScoreHistory(tenantId, request, response);
        log.debug("Publish history id " + scoreHistory.getId() + "record " + scoreHistory.getIdType() + " "
                + scoreHistory.getRecordId() + " latticeId " + scoreHistory.getLatticeId());
        publishScoreHistory(scoreHistory);
    }

    @Override
    public void publish(String tenantId, ScoreRequest request, ScoreResponse response) {
        String modelId = request.getModelId();
        if (modelId == null) {
            // to avoid issues in serialization with null key, we are using this
            // default model id
            modelId = "_NO_MODEL_ID_";
        }

        Record record = new Record();
        record.setRecordId(response.getId());
        record.setIdType("Unknown");
        record.setPerformEnrichment(false);
        record.setRootOperationId("");
        record.setRule(request.getRule());
        record.setRequestTimestamp(response.getTimestamp());
        Map<String, Map<String, Object>> attrMap = new HashMap<String, Map<String, Object>>();
        attrMap.put(modelId, request.getRecord());
        record.setModelAttributeValuesMap(attrMap);

        RecordScoreResponse recordRsp = new RecordScoreResponse();
        recordRsp.setId(response.getId());
        recordRsp.setLatticeId(response.getLatticeId());
        List<RecordScoreResponse.ScoreModelTuple> scores = new ArrayList<RecordScoreResponse.ScoreModelTuple>();
        RecordScoreResponse.ScoreModelTuple tuple = new RecordScoreResponse.ScoreModelTuple();
        tuple.setModelId(modelId);
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
        if (StringUtils.isBlank(history.getRequestTimestamp())) {
            history.setRequestTimestamp(timestampFormatter.print(DateTime.now(DateTimeZone.UTC)));
        }
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
