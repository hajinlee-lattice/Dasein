package com.latticeengines.scoringapi.score.impl;

import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map;

import com.latticeengines.domain.exposed.scoringapi.Record;
import com.latticeengines.scoringapi.exposed.InterpretedFields;

public class RecordModelTuple {
    private String requstTimestamp;
    private String latticeId;
    private Record record;
    private AbstractMap.SimpleEntry<Map<String, Object>, InterpretedFields> parsedData;
    private String modelId;

    public RecordModelTuple(String requstTimestamp, String latticeId, Record record,
            SimpleEntry<Map<String, Object>, InterpretedFields> parsedData, String modelId) {
        this.requstTimestamp = requstTimestamp;
        this.latticeId = latticeId;
        this.record = record;
        this.parsedData = parsedData;
        this.modelId = modelId;
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
}