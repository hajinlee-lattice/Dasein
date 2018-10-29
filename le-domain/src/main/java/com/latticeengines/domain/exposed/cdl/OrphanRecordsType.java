package com.latticeengines.domain.exposed.cdl;

public enum OrphanRecordsType {
    CONTACT("OrphanContacts", "Orphan Contacts", "orphanContactExportFlow"),
    TRANSACTION("OrphanTransactions", "Orphan Transactions", "orphanTransactionExportFlow"),
    UNMATCHED_ACCOUNT("UnmatchedAccount", "Unmatched Accounts", "unmatchedAccountExportFlow");

    private String orphanType;
    private String displayName;
    private String beanName;

    OrphanRecordsType(String orphanType, String displayName, String beanName) {
        this.orphanType = orphanType;
        this.displayName = displayName;
        this.beanName = beanName;
    }

    public String getOrphanType() {
        return this.orphanType;
    }

    public String getDisplayName() {
        return this.displayName;
    }

    public String getBeanName() {
        return this.beanName;
    }
}
