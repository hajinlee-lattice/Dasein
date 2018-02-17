package com.latticeengines.domain.exposed.metadata.datafeed;

import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed.Status;

public enum DataFeedExecutionJobType {
    CDLOperation(Status.Deleting), //
    Import(Status.InitialLoaded), //
    PA(Status.ProcessAnalyzing); //

    private final Status status;

    private DataFeedExecutionJobType(Status status) {
        this.status = status;
    }

    public Status getRunningStatus() {
        return status;
    }
}
