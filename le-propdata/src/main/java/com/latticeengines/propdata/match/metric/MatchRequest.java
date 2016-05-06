package com.latticeengines.propdata.match.metric;

import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.common.exposed.metric.RetentionPolicy;
import com.latticeengines.domain.exposed.monitor.metric.BaseMeasurement;
import com.latticeengines.domain.exposed.monitor.metric.RetentionPolicyImpl;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;

public class MatchRequest extends BaseMeasurement<MatchInput, MatchInput>
        implements Measurement<MatchInput, MatchInput> {

    private MatchInput matchInput;

    public MatchRequest(MatchInput matchInput, Integer numSelectedColumns) {
        matchInput.setNumSelectedColumns(numSelectedColumns);
        setMatchInput(matchInput);
    }

    public MatchInput getMatchInput() {
        return matchInput;
    }

    public void setMatchInput(MatchInput matchInput) {
        this.matchInput = matchInput;
    }

    @Override
    public RetentionPolicy getRetentionPolicy() {
        return RetentionPolicyImpl.ONE_WEEK;
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
