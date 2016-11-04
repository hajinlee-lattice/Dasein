package com.latticeengines.datacloud.match.metric;


import java.util.Arrays;
import java.util.Collection;

import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.common.exposed.metric.MetricStore;
import com.latticeengines.common.exposed.metric.RetentionPolicy;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.domain.exposed.monitor.metric.BaseMeasurement;
import com.latticeengines.domain.exposed.monitor.metric.MetricStoreImpl;
import com.latticeengines.domain.exposed.monitor.metric.RetentionPolicyImpl;

public class FuzzyMatchHistory extends BaseMeasurement<MatchTraveler, MatchTraveler>
        implements Measurement<MatchTraveler, MatchTraveler> {

    private final MatchTraveler traveler;

    public FuzzyMatchHistory(MatchTraveler traveler) {
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

    @SuppressWarnings("unchecked")
    @Override
    public Collection<MetricStore> getMetricStores() {
        return Arrays.<MetricStore>asList(MetricStoreImpl.INFLUX_DB, MetricStoreImpl.SPLUNK_LOG);
    }

}
