package com.latticeengines.propdata.match.metric;

import java.util.Arrays;
import java.util.Collection;

import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.common.exposed.metric.MetricStore;
import com.latticeengines.domain.exposed.monitor.metric.BaseMeasurement;
import com.latticeengines.domain.exposed.monitor.metric.MetricStoreImpl;
import com.latticeengines.domain.exposed.propdata.match.InputAccount;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchKeyDimension;
import com.latticeengines.domain.exposed.propdata.match.Matched;
import com.latticeengines.propdata.match.service.impl.MatchContext;

public class MatchedAccount extends BaseMeasurement<Matched, InputAccount>
        implements Measurement<Matched, InputAccount> {

    private Matched fact;
    private InputAccount dimension;

    public MatchedAccount(MatchInput input, MatchKeyDimension keyDimension, MatchContext.MatchEngine matchEngine,
            Boolean matched) {
        this.dimension = new InputAccount(input, keyDimension);
        if (matchEngine != null) {
            this.dimension.setMatchEngine(matchEngine.getName());
        }
        this.fact = new Matched(matched);
    }

    @Override
    public Collection<MetricStore> getMetricStores() {
        return Arrays.asList((MetricStore) MetricStoreImpl.INFLUX_DB, MetricStoreImpl.SPLUNK_LOG);
    }

    @Override
    public Matched getFact() {
        return fact;
    }

    public void setFact(Matched fact) {
        this.fact = fact;
    }

    @Override
    public InputAccount getDimension() {
        return dimension;
    }

    public void setDimension(InputAccount dimension) {
        this.dimension = dimension;
    }
}
