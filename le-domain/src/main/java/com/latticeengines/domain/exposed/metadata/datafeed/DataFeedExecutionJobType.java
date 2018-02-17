package com.latticeengines.domain.exposed.metadata.datafeed;

import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed.Status;

public enum DataFeedExecutionJobType {
    CDLOperation {
        @Override
        public Status getRunningStatus() {
            return Status.Deleting;
        }
    }, //
    Import {
        @Override
        public Status getRunningStatus() {
            return Status.InitialLoaded;
        }
    }, //
    PA {
        @Override
        public Status getRunningStatus() {
            return Status.ProcessAnalyzing;
        }
    }; //

    public abstract Status getRunningStatus();
}
