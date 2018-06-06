package com.latticeengines.domain.exposed.serviceapps.cdl;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ActivityMetricsValidation {
    @JsonProperty
    private boolean disableAll;

    @JsonProperty
    private boolean disableShareOfWallet;

    @JsonProperty
    private boolean disableMargin;

    public boolean getDisableAll() {
        return disableAll;
    }

    public void setDisableAll(boolean disableAll) {
        this.disableAll = disableAll;
    }

    public boolean getDisableShareOfWallet() {
        return disableShareOfWallet;
    }

    public void setDisableShareOfWallet(boolean disableShareOfWallet) {
        this.disableShareOfWallet = disableShareOfWallet;
    }

    public boolean getDisableMargin() {
        return disableMargin;
    }

    public void setDisableMargin(boolean disableMargin) {
        this.disableMargin = disableMargin;
    }
}
