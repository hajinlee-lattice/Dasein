package com.latticeengines.datacloud.core.source.impl;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.metadata.Table;

/*
 * This is place holder for data sources created on the fly.
 */
public class TableSource implements Source {

    private static final long serialVersionUID = -6860566039835739113L;
    private static final CustomerSpace DEFAULT_TENANT = CustomerSpace.parse(DataCloudConstants.SERVICE_CUSTOMERSPACE);

    private Table table;

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

    public CustomerSpace getCustomerSpace() {
        return customerSpace;
    }

    /*
         * name of the source
         */
    public String getSourceName() {
        return table.getName();
    }

    /*
     * timestamp field for sorting
     */
    public String getTimestampField() {
        return table.getLastModifiedKey().getAttributesAsStr();
    }

    /*
     * primary key
     */
    public String[] getPrimaryKey() {
        return table.getPrimaryKey().getAttributeNames();
    }

    /*
     * cron expression used to specify frequency of source data engine for this
     * source
     */
    public String getDefaultCronExpression() {
         return null;
    }

}
