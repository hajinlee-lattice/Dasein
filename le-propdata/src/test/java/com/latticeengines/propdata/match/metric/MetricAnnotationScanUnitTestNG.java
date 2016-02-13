package com.latticeengines.propdata.match.metric;

import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.MetricUtils;
import com.latticeengines.propdata.match.service.impl.MatchContext;

public class MetricAnnotationScanUnitTestNG {

    @Test(groups = "unit")
    public void scanMeasurements() {
        MetricUtils.scanTags(MatchContext.class);
        MetricUtils.scanFields(MatchContext.class);

        MetricUtils.scan(MatchedAccount.class);
        MetricUtils.scan(MatchedColumn.class);
        MetricUtils.scan(RealTimeRequest.class);
        MetricUtils.scan(RealTimeResponse.class);
        MetricUtils.scan(SqlQueryMeasurement.class);
    }

}
