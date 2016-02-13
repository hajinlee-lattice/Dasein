package com.latticeengines.domain.exposed.monitor.metric;

import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.MetricUtils;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.domain.exposed.propdata.match.MatchStatistics;

public class MetricAnnotationScanUnitTestNG {

    @Test(groups = "unit")
    public void testMetricDimension() {
        MetricUtils.scanTags(MatchInput.class);
        MetricUtils.scanFields(MatchInput.class);

        MetricUtils.scanTags(MatchOutput.class);
        MetricUtils.scanFields(MatchOutput.class);

        MetricUtils.scanFields(MatchStatistics.class);

        MetricUtils.scanTags(SqlQueryMetric.class);
        MetricUtils.scanFields(SqlQueryMetric.class);
    }

}
