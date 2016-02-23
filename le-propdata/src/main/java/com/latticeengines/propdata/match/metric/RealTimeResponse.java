package com.latticeengines.propdata.match.metric;

import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.domain.exposed.monitor.metric.BaseMeasurement;
import com.latticeengines.propdata.match.service.impl.MatchContext;

public class RealTimeResponse extends BaseMeasurement<MatchContext, MatchContext>
        implements Measurement<MatchContext, MatchContext> {

    private MatchContext context;

    public RealTimeResponse(MatchContext context) {
        this.context = context;
    }

    public MatchContext getContext() {
        return context;
    }

    public void setContext(MatchContext context) {
        this.context = context;
    }

    @Override
    public MatchContext getFact() {
        return getContext();
    }

    @Override
    public MatchContext getDimension() {
        return getContext();
    }

}
