package com.latticeengines.propdata.match.metric;

import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.domain.exposed.monitor.metric.BaseMeasurement;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.propdata.match.service.impl.MatchContext;

public class RealTimeRequest extends BaseMeasurement<MatchInput, MatchInput>
        implements Measurement<MatchInput, MatchInput> {

    private MatchInput matchInput;

    public RealTimeRequest(MatchInput matchInput, MatchContext.MatchEngine matchEngine, Integer numSelectedColumns) {
        this.matchInput = matchInput;
        if (matchEngine != null) {
            this.matchInput.setMatchEngine(matchEngine.getName());
        }
        this.matchInput.setNumSelectedColumns(numSelectedColumns);
    }

    public MatchInput getMatchInput() {
        return matchInput;
    }

    public void setMatchInput(MatchInput matchInput) {
        this.matchInput = matchInput;
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
