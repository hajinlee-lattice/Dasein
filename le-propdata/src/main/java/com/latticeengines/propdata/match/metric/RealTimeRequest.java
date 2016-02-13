package com.latticeengines.propdata.match.metric;

import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.common.exposed.metric.RetentionPolicy;
import com.latticeengines.domain.exposed.monitor.metric.RetentionPolicyImpl;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.propdata.match.service.impl.MatchContext;

public class RealTimeRequest implements Measurement<MatchInput, MatchInput> {

    private MatchInput matchInput;

    public RealTimeRequest(MatchInput matchInput, MatchContext.MatchEngine matchEngine, Integer numSelectedColumns) {
        this.matchInput = matchInput;
        this.matchInput.setMatchEngine(matchEngine.getName());
        this.matchInput.setNumSelectedColumns(numSelectedColumns);
    }

    public MatchInput getMatchInput() {
        return matchInput;
    }

    public void setMatchInput(MatchInput matchInput) {
        this.matchInput = matchInput;
    }

    @Override
    public RetentionPolicy getRetentionPolicy() {
        return RetentionPolicyImpl.DEFAULT;
    }

    @Override
    public MatchInput getFact() {
        return getMatchInput();
    }

    @Override
    public MatchInput getDimension() {
        return getMatchInput();
    }

}
