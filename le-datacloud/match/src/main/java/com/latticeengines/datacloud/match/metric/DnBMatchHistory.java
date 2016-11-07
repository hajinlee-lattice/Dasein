package com.latticeengines.datacloud.match.metric;

import java.util.Arrays;
import java.util.Collection;

import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.common.exposed.metric.MetricStore;
import com.latticeengines.common.exposed.metric.RetentionPolicy;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;
import com.latticeengines.domain.exposed.monitor.metric.BaseMeasurement;
import com.latticeengines.domain.exposed.monitor.metric.MetricStoreImpl;
import com.latticeengines.domain.exposed.monitor.metric.RetentionPolicyImpl;

public class DnBMatchHistory extends BaseMeasurement<DnBMatchContext, DnBMatchContext>
        implements Measurement<DnBMatchContext, DnBMatchContext> {

    private final DnBMatchContext dnBMatchContext;

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

    @SuppressWarnings("unchecked")
    @Override
    public Collection<MetricStore> getMetricStores() {
        return Arrays.<MetricStore>asList(MetricStoreImpl.INFLUX_DB, MetricStoreImpl.SPLUNK_LOG);
    }

}
