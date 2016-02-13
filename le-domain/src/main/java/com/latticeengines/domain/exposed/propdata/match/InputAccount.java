package com.latticeengines.domain.exposed.propdata.match;

import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;
import com.latticeengines.common.exposed.metric.annotation.MetricTagGroup;

public class InputAccount implements Dimension {

    private String matchEngine;
    private MatchInput input;
    private MatchKeyDimension keyDimension;

    public InputAccount(MatchInput matchInput, MatchKeyDimension keyDimension) {
        this.input = matchInput;
        this.keyDimension = keyDimension;
    }

    @MetricTag(tag = "MatchEngine")
    public String getMatchEngine() {
        return matchEngine;
    }

    public void setMatchEngine(String matchEngine) {
        this.matchEngine = matchEngine;
    }

    @MetricTagGroup(includes = { "TenantId" })
    public MatchInput getInput() {
        return input;
    }

    public void setInput(MatchInput input) {
        this.input = input;
    }

    @MetricTagGroup
    public MatchKeyDimension getKeyDimension() {
        return keyDimension;
    }

    public void setKeyDimension(MatchKeyDimension keyDimension) {
        this.keyDimension = keyDimension;
    }

}
