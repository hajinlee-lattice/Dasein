package com.latticeengines.domain.exposed.pls;

public enum RatingEngineType {
    RULE_BASED("RuleBased"), //
    CROSS_SELL("Cross-Sell"), //
    CUSTOM_EVENT("Custom-Event"), //
    ACCOUNT_PROSPECTING("Account-Prospecting"), //
    PRODUCT_PROSPECTING("Product-Prospecting");

    private String name;

    private RatingEngineType(String name) {
        this.name = name;
    }

    public String getRatingEngineTypeName() {
        return this.name;
    }

}
