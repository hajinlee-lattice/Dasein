package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.OrphanRecordsType;
import com.latticeengines.domain.exposed.metadata.DataCollection;

public class ComputeOrphanRecordsStepConfiguration extends BaseCDLDataFlowStepConfiguration {
    public static final String DEPENDS_ON_ORPHAN_TYPE = "DEPENDS_ON_ORPHAN_TYPE";

    private String orphanRecordsExportId;
    private String dataCollectionName;
    private DataCollection.Version dataCollectionVersion;
    private OrphanRecordsType orphanRecordsType;

    private String transactionTableName;
    private String accountTableName;
    private String productTableName;
    private String contactTableName;
    private List<String> validatedColumns;

    @Override
    public void setBeanName(String beanName) {
        if (beanName.equals(DEPENDS_ON_ORPHAN_TYPE)) {
            super.setBeanName(orphanRecordsType.getBeanName());
        } else {
            super.setBeanName(beanName);
        }
    }

    public String getTransactionTableName() {
        return transactionTableName;
    }

    public void setTransactionTableName(String transactionTableName) {
        this.transactionTableName = transactionTableName;
    }

    public String getAccountTableName() {
        return accountTableName;
    }

    public void setAccountTableName(String accountTableName) {
        this.accountTableName = accountTableName;
    }

    public String getProductTableName() {
        return productTableName;
    }

    public void setProductTableName(String productTableName) {
        this.productTableName = productTableName;
    }

    public String getContactTableName() {
        return contactTableName;
    }

    public void setContactTableName(String contactTableName) {
        this.contactTableName = contactTableName;
    }

    public String getOrphanRecordsExportId() {
        return orphanRecordsExportId;
    }

    public void setOrphanRecordsExportId(String orphanRecordsExportId) {
        this.orphanRecordsExportId = orphanRecordsExportId;
    }

    public String getDataCollectionName() {
        return dataCollectionName;
    }

    public void setDataCollectionName(String dataCollectionName) {
        this.dataCollectionName = dataCollectionName;
    }

    public DataCollection.Version getDataCollectionVersion() {
        return dataCollectionVersion;
    }

    public void setDataCollectionVersion(DataCollection.Version dataCollectionVersion) {
        this.dataCollectionVersion = dataCollectionVersion;
    }

    public OrphanRecordsType getOrphanRecordsType() {
        return this.orphanRecordsType;
    }

    public void setOrphanRecordsType(OrphanRecordsType type) {
        this.orphanRecordsType = type;
    }

    public void setValidatedColumns(List<String> validatedColumns) {
        this.validatedColumns = validatedColumns;
    }

    public List<String> getValidatedColumns() {
        return validatedColumns;
    }

}
