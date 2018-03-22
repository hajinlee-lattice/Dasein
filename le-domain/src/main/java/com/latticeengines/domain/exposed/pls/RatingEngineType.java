package com.latticeengines.domain.exposed.pls;

public enum RatingEngineType {
    RULE_BASED("RuleBased"), //
    CROSS_SELL("Cross-Sell"), //
    CUSTOM_EVENT("Custom-Event", false), //
    ACCOUNT_PROSPECTING("Account-Prospecting"), //
    PRODUCT_PROSPECTING("Product-Prospecting");

    private String name;
    private boolean isTargetSegmentMandatory;

    private RatingEngineType(String name) {
        this(name, true);
    }

    private RatingEngineType(String name, boolean isTargetSegmentMandatory) {
        this.name = name;
        this.isTargetSegmentMandatory = isTargetSegmentMandatory;
    }

    public String getRatingEngineTypeName() {
        return this.name;
    }

    public boolean isTargetSegmentMandatory() {
        return isTargetSegmentMandatory;
    }

}
