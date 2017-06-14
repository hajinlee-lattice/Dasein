package com.latticeengines.datacloud.core.source.impl;

import org.apache.commons.lang3.StringUtils;

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
    private boolean expandBucketedAttrs = false;

    private final CustomerSpace customerSpace;

    public TableSource(Table table) {
        this(table, DEFAULT_TENANT);
    }

    public TableSource(Table table, CustomerSpace customerSpace) {
        this.table = table;
        this.customerSpace = customerSpace;
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
        return customerSpace.toString() + "-" + tableName;
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
        if (StringUtils.isNotBlank(primaryKey)) {
            return new String[]{primaryKey};
        } else if (table.getPrimaryKey() != null) {
            return table.getPrimaryKey().getAttributeNames();
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

}
