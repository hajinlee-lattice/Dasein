package com.latticeengines.domain.exposed.cdl.activity;

import java.io.Serializable;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ActivityBookkeeping implements Serializable {

    private static final long serialVersionUID = 0L;

    // streamId -> records (dateId -> count)
    public Map<String, Map<Integer, Long>> streamRecord;

    public ActivityBookkeeping() {
    }
}
