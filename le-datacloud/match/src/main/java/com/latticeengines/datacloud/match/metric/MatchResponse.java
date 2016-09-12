package com.latticeengines.datacloud.match.metric;

import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.common.exposed.metric.RetentionPolicy;
import com.latticeengines.datacloud.match.service.impl.MatchContext;
import com.latticeengines.domain.exposed.monitor.metric.BaseMeasurement;
import com.latticeengines.domain.exposed.monitor.metric.RetentionPolicyImpl;

public class MatchResponse extends BaseMeasurement<MatchContext, MatchContext>
        implements Measurement<MatchContext, MatchContext> {

    private MatchContext context;

    public MatchResponse(MatchContext context) {
        this.context = context;
    }

    public MatchContext getContext() {
        return context;
    }

    public void setContext(MatchContext context) {
        this.context = context;
    }

    @Override
    public RetentionPolicy getRetentionPolicy() {
        return RetentionPolicyImpl.ONE_WEEK;
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
