package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

public class CdlPivotScoreAndEventConfiguration extends BaseCDLDataFlowStepConfiguration {

    private boolean expectedValue;

    public CdlPivotScoreAndEventConfiguration() {
        setBeanName("cdlPivotScoreAndEvent");
    }

    public boolean isExpectedValue() {
        return expectedValue;
    }

    public void setExpectedValue(boolean expectedValue) {
        this.expectedValue = expectedValue;
    }

}
