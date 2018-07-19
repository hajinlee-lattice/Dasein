package com.latticeengines.domain.exposed.serviceapps.cdl;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class DataCollectionPrechecks {
    @JsonProperty
    private boolean disableAllCuratedMetrics;

    @JsonProperty
    private boolean disableShareOfWallet;

    @JsonProperty
    private boolean disableMargin;

    @JsonProperty
    private boolean disableCrossSellModeling;

    public boolean getDisableAllCuratedMetrics() {
        return disableAllCuratedMetrics;
    }

    public void setDisableAllCuratedMetrics(boolean disableAllCuratedMetrics) {
        this.disableAllCuratedMetrics = disableAllCuratedMetrics;
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

    public boolean getDisableCrossSellModeling() {
        return disableCrossSellModeling;
    }

    public void setDisableCrossSellModeling(boolean disableCrossSellModeling) {
        this.disableCrossSellModeling = disableCrossSellModeling;
    }
}
