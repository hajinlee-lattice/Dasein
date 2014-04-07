package com.latticeengines.scoring.exposed.domain;

import java.util.Map;

public class ScoringRequest {
    private String id = null;

    private Map<String, ?> arguments = null;

    public ScoringRequest() {
    }

    public ScoringRequest(String id) {
        setId(id);
    }

    public Object getArgument(String key) {
        return arguments.get(key);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Map<String, ?> getArguments() {
        return arguments;
    }

    public void setArguments(Map<String, ?> arguments) {
        this.arguments = arguments;
    }

}
