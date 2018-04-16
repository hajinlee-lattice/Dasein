package com.latticeengines.scoringapi.exposed.context;

import org.testng.Assert;
import org.testng.annotations.Test;

public class SingleEnrichRecordMeasurementUnitNG extends EnrichRequestMetricsUnitTestNG {

    private SingleEnrichRecordMeasurement measurement;

    @Test(groups = "unit")
    public void testBasicOperations() {
        measurement = new SingleEnrichRecordMeasurement(requestMetrics);
        Assert.assertNotNull(measurement.getDimension());
        Assert.assertNotNull(measurement.getFact());
        Assert.assertEquals(measurement.getFact(), requestMetrics);
    }

}
