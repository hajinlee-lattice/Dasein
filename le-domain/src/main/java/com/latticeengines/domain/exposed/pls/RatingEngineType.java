package com.latticeengines.domain.exposed.pls;

public enum RatingEngineType {
    RULE_BASED("RuleBased"), //
    AI_BASED("AiBased");

    private String name;

    private RatingEngineType(String name) {
        this.name = name;
    }

    public String getRatingEngineTypeName() {
        return this.name;
    }

}
