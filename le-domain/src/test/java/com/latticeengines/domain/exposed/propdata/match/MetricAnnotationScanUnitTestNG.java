package com.latticeengines.domain.exposed.propdata.match;

import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.MetricUtils;

public class MetricAnnotationScanUnitTestNG {

    @Test(groups = "unit")
    public void testMetricDimension() {
        MetricUtils.scanTags(MatchInput.class);
        MetricUtils.scanTags(MatchOutput.class);

        MetricUtils.scanFields(MatchInput.class);
        MetricUtils.scanFields(MatchOutput.class);
        MetricUtils.scanFields(MatchStatistics.class);
    }

}
