package com.latticeengines.datacloud.core.source.impl;

import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.util.TableUtils;

/*
 * This is place holder for data sources created on the fly.
 */
public class TableSource implements Source {

    private static final long serialVersionUID = -6860566039835739113L;
    private static final CustomerSpace DEFAULT_TENANT = CustomerSpace.parse(DataCloudConstants.SERVICE_CUSTOMERSPACE);

    private Table table;
    private String primaryKey;
    private String lastModifiedKey;
    private List<String> partitionKeys;
    private boolean expandBucketedAttrs = false;

    private final CustomerSpace customerSpace;

    public TableSource(Table table) {
        this(table, DEFAULT_TENANT);
    }

    public TableSource(Table table, CustomerSpace customerSpace) {
        this(table, customerSpace, null);
    }

    public TableSource(Table table, CustomerSpace customerSpace, List<String> partitionKeys) {
        this.table = table;
        this.customerSpace = customerSpace;
        this.partitionKeys = partitionKeys;
    }

    public Table getTable() {
        return table;
    }

    public void setTable(Table table) {
        this.table = table;
    }

    public boolean isExpandBucketedAttrs() {
        return expandBucketedAttrs;
    }

    public void setExpandBucketedAttrs(boolean expandBucketedAttrs) {
        this.expandBucketedAttrs = expandBucketedAttrs;
    }

    public CustomerSpace getCustomerSpace() {
        return customerSpace;
    }

    public void setPrimaryKey(String primaryKey) {
        this.primaryKey = primaryKey;
    }

    public String getLastModifiedKey() {
        return lastModifiedKey;
    }

    public void setLastModifiedKey(String lastModifiedKey) {
        this.lastModifiedKey = lastModifiedKey;
    }

    /*
         * name of the source
         */
    public String getSourceName() {
        return getSourceName(customerSpace, table.getName());
    }

    public static String getSourceName(CustomerSpace customerSpace, String tableName) {
        String name = customerSpace.getTenantId() + "_" + tableName;
        return AvroUtils.getAvroFriendlyString(name);
    }

    /*
     * timestamp field for sorting
     */
    public String getTimestampField() {
        if (StringUtils.isNotBlank(lastModifiedKey)) {
            return lastModifiedKey;
        } else if (table.getLastModifiedKey() != null) {
            return table.getLastModifiedKey().getAttributesAsStr();
        } else {
            return null;
        }
    }

    /*
     * primary key
     */
    public String[] getPrimaryKey() {
        if (table.getPrimaryKey() != null) {
            return table.getPrimaryKey().getAttributeNames();
        } else if (StringUtils.isNotBlank(primaryKey)) {
            return new String[]{ primaryKey };
        } else {
            return null;
        }
    }

    public String getSinglePrimaryKey() {
        if (table.getPrimaryKey() != null) {
            return table.getPrimaryKey().getAttributesAsStr();
        } else if (StringUtils.isNotBlank(primaryKey)) {
            return primaryKey;
        } else {
            return null;
        }
    }

    /*
     * cron expression used to specify frequency of source data engine for this
     * source
     */
    public String getDefaultCronExpression() {
        return null;
    }

    public static String getFullTableName(String tableNamePrefix, String version) {
        return TableUtils.getFullTableName(tableNamePrefix, version);
    }

    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    public void setPartitionKeys(List<String> partitionKeys) {
        this.partitionKeys = partitionKeys;
    }
}
