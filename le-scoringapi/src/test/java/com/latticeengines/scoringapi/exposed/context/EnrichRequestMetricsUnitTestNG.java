package com.latticeengines.scoringapi.exposed.context;

import org.testng.Assert;
import org.testng.annotations.Test;

public class EnrichRequestMetricsUnitTestNG {

    protected EnrichRequestMetrics requestMetrics = new EnrichRequestMetrics();;

    private Integer parseUuidDurationMS = 12345;

    private String tenantId = "tenant";
    private Boolean hasWarning = Boolean.FALSE;
    private Boolean isEnrich = Boolean.TRUE;
    private String source = "source";

    private Integer requestPreparationDurationMS = 12345;
    private Integer matchRecordDurationMS = 12345;
    private Integer requestDurationMS = 12345;

    @Test(groups = "unit")
    public void testBasicOperations() {
        requestMetrics.setHasWarning(hasWarning);
        requestMetrics.setIsEnrich(isEnrich);
        requestMetrics.setRequestPreparationDurationMS(requestPreparationDurationMS);
        requestMetrics.setMatchRecordDurationMS(matchRecordDurationMS);
        requestMetrics.setParseUuidDurationMS(parseUuidDurationMS);
        requestMetrics.setRequestDurationMS(requestDurationMS);
        requestMetrics.setSource(source);
        requestMetrics.setTenantId(tenantId);
        Assert.assertEquals(requestMetrics.isEnrich(), isEnrich.toString());
        Assert.assertEquals(requestMetrics.hasWarning(), hasWarning.toString());
        Assert.assertEquals(requestMetrics.getSource(), source);
        Assert.assertEquals(requestMetrics.getTenantId(), tenantId);
        Assert.assertEquals(requestMetrics.getMatchRecordDurationMS(), matchRecordDurationMS);
        Assert.assertEquals(requestMetrics.getRequestDurationMS(), requestDurationMS);
        Assert.assertEquals(requestMetrics.getParseUuidDurationMS(), parseUuidDurationMS);
        Assert.assertEquals(requestMetrics.getRequestPreparationDurationMS(), requestPreparationDurationMS);
    }

}
