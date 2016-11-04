package com.latticeengines.datacloud.match.metric;

import java.util.Arrays;
import java.util.Collection;

import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.common.exposed.metric.MetricStore;
import com.latticeengines.common.exposed.metric.RetentionPolicy;
import com.latticeengines.datacloud.match.actors.visitor.MatchTraveler;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchOutput;
import com.latticeengines.domain.exposed.monitor.metric.BaseMeasurement;
import com.latticeengines.domain.exposed.monitor.metric.MetricStoreImpl;
import com.latticeengines.domain.exposed.monitor.metric.RetentionPolicyImpl;

public class DnBMatchHistory extends BaseMeasurement<DnBMatchOutput, MatchTraveler>
        implements Measurement<DnBMatchOutput, MatchTraveler> {

    private final MatchTraveler traveler;

    public DnBMatchHistory(MatchTraveler traveler) {
        this.traveler = traveler;
    }

    @Override
    public DnBMatchOutput getFact() {
        return this.traveler.getDnBMatchOutput();
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
