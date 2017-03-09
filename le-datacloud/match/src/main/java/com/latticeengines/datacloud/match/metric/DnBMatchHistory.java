package com.latticeengines.datacloud.match.metric;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.common.exposed.metric.RetentionPolicy;
import com.latticeengines.common.exposed.util.MetricUtils;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.monitor.metric.BaseMeasurement;
import com.latticeengines.domain.exposed.monitor.metric.RetentionPolicyImpl;

public class DnBMatchHistory extends BaseMeasurement<DnBMatchContext, DnBMatchContext>
        implements Measurement<DnBMatchContext, DnBMatchContext> {

    private final DnBMatchContext dnBMatchContext;
    private static final Set<String> excludedSystemTags = Collections.singleton(MetricUtils.TAG_HOST);

    public DnBMatchHistory(DnBMatchContext dnBMatchContext) {
        this.dnBMatchContext = dnBMatchContext;
    }

    @Override
    public DnBMatchContext getFact() {
        return this.dnBMatchContext;
    }

    @Override
    public DnBMatchContext getDimension() {
        return this.dnBMatchContext;
    }

    @Override
    public RetentionPolicy getRetentionPolicy() {
        return RetentionPolicyImpl.ONE_MONTH;
    }

    @Override
    public Collection<String> excludeSystemTags() {
        return excludedSystemTags;
    }

}
