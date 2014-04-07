package com.latticeengines.scoring.exposed.domain;

import java.util.Map;

public class ScoringResponse {
    private String id = null;

    private Map<String, ?> result = null;

    public ScoringResponse() {
    }

    public ScoringResponse(String id) {
        setId(id);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Map<String, ?> getResult() {
        return result;
    }

    public void setResult(Map<String, ?> result) {
        this.result = result;
    }

}
