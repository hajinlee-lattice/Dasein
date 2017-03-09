package com.latticeengines.domain.exposed.pls;

public enum VdbCreateTableRule {
    UPDATE("Update"),
    CREATE("Create"),
    CREATE_NEW("CreateNew");

    private String createRule;

    VdbCreateTableRule(String createRule) {
        this.createRule = createRule;
    }

    public String getCreateRule() {
        return createRule;
    }

    public static VdbCreateTableRule getCreateRule(String createRule) {
        VdbCreateTableRule result = null;
        for (VdbCreateTableRule rule : VdbCreateTableRule.values()) {
            if (createRule.toLowerCase().equals(rule.getCreateRule().toLowerCase())) {
                result = rule;
                break;
            }
        }
        return result;
    }
}
