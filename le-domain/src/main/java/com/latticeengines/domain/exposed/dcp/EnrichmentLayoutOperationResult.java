package com.latticeengines.domain.exposed.dcp;

public class EnrichmentLayoutOperationResult {

    boolean valid;
    String message;
    String layoutId;

    public EnrichmentLayoutOperationResult(boolean valid, String message) {
        this.valid = valid;
        this.message = message;
    }

    public EnrichmentLayoutOperationResult(boolean valid, String message, String layoutId) {
        this.valid = valid;
        this.message = message;
        this.layoutId = layoutId;
    }

    public boolean isValid() {
        return valid;
    }

    public String getLayoutId() {
        return layoutId;
    }

    public void setLayoutId(String layoutId) {
        this.layoutId = layoutId;
    }
}
