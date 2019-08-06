package com.latticeengines.domain.exposed.cdl.scheduling;

import com.google.common.base.Preconditions;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;

/*
 * Contain all scheduler related information for a specific tenant
 */
public class SchedulingStatus {
    private String customerSpace;
    private boolean schedulerEnabled;
    private DataFeed dataFeed;
    private DataFeedExecution latestExecution;
    private boolean pendingRetry; // whether scheduling is waiting to retry PA for this tenant

    public SchedulingStatus() {
    }

    public SchedulingStatus(@NotNull String customerSpace, boolean schedulerEnabled, DataFeed dataFeed,
            DataFeedExecution latestExecution, boolean pendingRetry) {
        Preconditions.checkNotNull(customerSpace, "Customerspace should not be null");
        this.customerSpace = customerSpace;
        this.schedulerEnabled = schedulerEnabled;
        this.dataFeed = dataFeed;
        this.latestExecution = latestExecution;
        this.pendingRetry = pendingRetry;
    }

    public String getCustomerSpace() {
        return customerSpace;
    }

    public boolean isSchedulerEnabled() {
        return schedulerEnabled;
    }

    public DataFeed getDataFeed() {
        return dataFeed;
    }

    public DataFeedExecution getLatestExecution() {
        return latestExecution;
    }

    public void setCustomerSpace(String customerSpace) {
        this.customerSpace = customerSpace;
    }

    public void setSchedulerEnabled(boolean schedulerEnabled) {
        this.schedulerEnabled = schedulerEnabled;
    }

    public void setDataFeed(DataFeed dataFeed) {
        this.dataFeed = dataFeed;
    }

    public void setLatestExecution(DataFeedExecution latestExecution) {
        this.latestExecution = latestExecution;
    }

    public boolean isPendingRetry() {
        return pendingRetry;
    }

    public void setPendingRetry(boolean pendingRetry) {
        this.pendingRetry = pendingRetry;
    }

    @Override
    public String toString() {
        return "SchedulingStatus{" + "customerSpace='" + customerSpace + '\'' + ", schedulerEnabled=" + schedulerEnabled
                + ", dataFeed=" + dataFeed + ", latestExecution=" + latestExecution + ", pendingRetry=" + pendingRetry
                + '}';
    }
}
