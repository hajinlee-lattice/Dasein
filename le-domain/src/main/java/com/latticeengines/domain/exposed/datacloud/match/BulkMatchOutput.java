package com.latticeengines.domain.exposed.datacloud.match;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricField;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;

public class BulkMatchOutput implements Fact, Dimension {
    private String requestId;
    private List<MatchOutput> outputList;
    private long timeElapsed;
    private String matchEngine;

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public List<MatchOutput> getOutputList() {
        return outputList;
    }

    public void setOutputList(List<MatchOutput> outputList) {
        this.outputList = outputList;
    }

    public long getTimeElapsed() {
        return timeElapsed;
    }

    public void setTimeElapsed(long timeElapsed) {
        this.timeElapsed = timeElapsed;
    }

    @MetricField(name = "TimeElapsed", fieldType = MetricField.FieldType.INTEGER)
    @JsonIgnore
    public Integer getTimeElapsedMetric() {
        return Integer.valueOf(String.valueOf(getTimeElapsed()));
    }

    @MetricField(name = "RowsRequested", fieldType = MetricField.FieldType.INTEGER)
    @JsonIgnore
    public Integer getRowsRequested() {
        int rows = 0;
        for (MatchOutput output: getOutputList()) {
            if (output.getResult() != null) {
                rows += output.getResult().size();
            }
        }
        return rows;
    }

    @MetricTag(tag = "MatchEngine")
    @JsonIgnore
    public String getMatchEngine() {
        return matchEngine;
    }

    @JsonIgnore
    public void setMatchEngine(String matchEngine) {
        this.matchEngine = matchEngine;
    }
}
