package com.latticeengines.domain.exposed.eai;

public enum ImportVdbTableMergeRule {
    APPEND(false), REPLACE(false), UPSERT_ID(true), UPSERT_ACPD(true), UPSERT_MINDATE(
            true), UPSERT_MINDATEANDACCOUNT(true);

    boolean needTempTable;

    ImportVdbTableMergeRule(boolean needTempTable) {
        this.needTempTable = needTempTable;
    }

    public static ImportVdbTableMergeRule getByName(String mergeRule) {
        for (ImportVdbTableMergeRule importVdbTableMergeRule : values()) {
            if (importVdbTableMergeRule.name().equalsIgnoreCase(mergeRule)) {
                return importVdbTableMergeRule;
            }
        }
        throw new IllegalArgumentException(
                String.format("There is no merge rule %s in ImportVdbTableMergeRule", mergeRule));
    }

    public boolean isNeedTempTable() {
        return needTempTable;
    }

    public void setNeedTempTable(boolean needTempTable) {
        this.needTempTable = needTempTable;
    }
}
