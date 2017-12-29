package com.latticeengines.domain.exposed.eai;

public enum ImportVdbTableMergeRule {
    APPEND,
    REPLACE;

    public static ImportVdbTableMergeRule getByName(String mergeRule) {
        for (ImportVdbTableMergeRule importVdbTableMergeRule : values()) {
            if (importVdbTableMergeRule.name().equalsIgnoreCase(mergeRule)) {
                return importVdbTableMergeRule;
            }
        }
        throw new IllegalArgumentException(String.format("There is no merge rule %s in ImportVdbTableMergeRule", mergeRule));
    }
}
