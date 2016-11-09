package com.latticeengines.datacloud.match.metric;

import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.common.exposed.metric.RetentionPolicy;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchOutput;
import com.latticeengines.domain.exposed.monitor.metric.BaseMeasurement;
import com.latticeengines.domain.exposed.monitor.metric.RetentionPolicyImpl;

public class BulkMatchResponse extends BaseMeasurement<BulkMatchOutput, BulkMatchOutput>
        implements Measurement<BulkMatchOutput, BulkMatchOutput> {

    private BulkMatchOutput output;

    public BulkMatchResponse(BulkMatchOutput output) {
        this.output = output;
    }

    public BulkMatchOutput getOutput() {
        return output;
    }

    public void setContext(BulkMatchOutput output) {
        this.output = output;
    }

    @Override
    public RetentionPolicy getRetentionPolicy() {
        return RetentionPolicyImpl.ONE_WEEK;
    }

    @Override
    public BulkMatchOutput getFact() {
        return getOutput();
    }

    @Override
    public BulkMatchOutput getDimension() {
        return getOutput();
    }

}
