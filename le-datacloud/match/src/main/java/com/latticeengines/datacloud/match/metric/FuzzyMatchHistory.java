package com.latticeengines.datacloud.match.metric;


import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.common.exposed.metric.RetentionPolicy;
import com.latticeengines.common.exposed.util.MetricUtils;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.domain.exposed.monitor.metric.BaseMeasurement;
import com.latticeengines.domain.exposed.monitor.metric.RetentionPolicyImpl;


public class FuzzyMatchHistory extends BaseMeasurement<MatchTraveler, MatchTraveler>
        implements Measurement<MatchTraveler, MatchTraveler> {

    private final MatchTraveler traveler;
    private static final Set<String> excludedSystemTags = Collections.singleton(MetricUtils.TAG_HOST);

    public FuzzyMatchHistory(MatchTraveler traveler) {
        traveler.recordTotalTime();
        this.traveler = traveler;
    }

    @Override
    public MatchTraveler getFact() {
        return this.traveler;
    }

    @Override
    public MatchTraveler getDimension() {
        return this.traveler;
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
