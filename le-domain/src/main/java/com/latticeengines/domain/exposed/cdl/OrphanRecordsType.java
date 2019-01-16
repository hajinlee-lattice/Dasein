package com.latticeengines.domain.exposed.cdl;

import com.latticeengines.domain.exposed.query.BusinessEntity;

public enum OrphanRecordsType {
    CONTACT("OrphanContacts", "Orphan Contacts", "orphanContactExportFlow",
            "File", "ContactData", BusinessEntity.Contact),

    TRANSACTION("OrphanTransactions", "Orphan Transactions", "orphanTransactionExportFlow",
            "File", "TransactionData", BusinessEntity.Transaction),

    UNMATCHED_ACCOUNT("UnmatchedAccount", "Unmatched Accounts", "unmatchedAccountExportFlow",
            "File", "AccountData", BusinessEntity.Account);

    private String orphanType;
    private String displayName;
    private String beanName;
    private String dataSource;
    private String dataFeedType;
    private BusinessEntity entity;

    OrphanRecordsType(String orphanType, String displayName, String beanName,
                      String dataSource, String dataFeedType, BusinessEntity entity) {
        this.orphanType = orphanType;
        this.displayName = displayName;
        this.beanName = beanName;
        this.dataSource = dataSource;
        this.dataFeedType = dataFeedType;
        this.entity = entity;
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

    public String getDataSource() {
        return this.dataSource;
    }

    public String getDataFeedType() {
        return this.dataFeedType;
    }

    public BusinessEntity getEntity() {
        return this.entity;
    }
}
