package com.latticeengines.domain.exposed.serviceflows.core.steps;

import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class ChooseModelStepConfiguration extends MicroserviceStepConfiguration {

    @NotNull
    private TargetMarket targetMarket;

    @NotEmptyString
    @NotNull
    private String internalResourceHostPort;

    public TargetMarket getTargetMarket() {
        return targetMarket;
    }

    public void setTargetMarket(TargetMarket targetMarket) {
        this.targetMarket = targetMarket;
    }

    public String getInternalResourceHostPort() {
        return internalResourceHostPort;
    }

    public void setInternalResourceHostPort(String internalResourceHostPort) {
        this.internalResourceHostPort = internalResourceHostPort;
    }

}
