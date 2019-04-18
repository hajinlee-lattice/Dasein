package com.latticeengines.scoringapi.score.impl;

import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map;

import com.latticeengines.domain.exposed.scoringapi.Record;
import com.latticeengines.scoringapi.exposed.InterpretedFields;
import com.latticeengines.scoringapi.exposed.exception.ScoringApiException;

public class RecordModelTuple {
    private String requstTimestamp;
    private String latticeId;
    private Record record;
    private AbstractMap.SimpleEntry<Map<String, Object>, InterpretedFields> parsedData;
    private String modelId;
    private ScoringApiException exception;

    public RecordModelTuple(String requstTimestamp, String latticeId, Record record,
            SimpleEntry<Map<String, Object>, InterpretedFields> parsedData, String modelId,
            ScoringApiException exception) {
        this.requstTimestamp = requstTimestamp;
        this.latticeId = latticeId;
        this.record = record;
        this.parsedData = parsedData;
        this.modelId = modelId;
        this.exception = exception;
    }

    public String getRequstTimestamp() {
        return requstTimestamp;
    }

    public String getLatticeId() {
        return latticeId;
    }

    public Record getRecord() {
        return record;
    }

    public AbstractMap.SimpleEntry<Map<String, Object>, InterpretedFields> getParsedData() {
        return parsedData;
    }

    public String getModelId() {
        return modelId;
    }

    public ScoringApiException getException() {
        return exception;
    }

    public void setException(ScoringApiException exception) {
        // do not overwrite original exception obj
        if (this.exception == null) {
            this.exception = exception;
        }
    }
}
