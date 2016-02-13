package com.latticeengines.propdata.match.metric;

import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.common.exposed.metric.RetentionPolicy;
import com.latticeengines.domain.exposed.monitor.metric.RetentionPolicyImpl;
import com.latticeengines.domain.exposed.monitor.metric.SqlQueryMetric;

public class FetchDataFromSql implements Measurement<SqlQueryMetric, SqlQueryMetric> {

    private SqlQueryMetric metric;

    public FetchDataFromSql(SqlQueryMetric metric) {
        this.metric = metric;
    }

    public SqlQueryMetric getMetric() {
        return metric;
    }

    public void setMetric(SqlQueryMetric metric) {
        this.metric = metric;
    }

    @Override
    public SqlQueryMetric getFact() {
        return getMetric();
    }

    @Override
    public SqlQueryMetric getDimension() {
        return getMetric();
    }

    @Override
    public RetentionPolicy getRetentionPolicy() {
        return RetentionPolicyImpl.ONE_WEEK;
    }
}
