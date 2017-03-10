package com.latticeengines.redshiftdb.load;

import org.joda.time.DateTime;

public class ResultMetrics {

    public ResultMetrics(DateTime start, long runningtime) {
        startTime = start;
        runtime = runningtime;
    }

    private DateTime startTime;
    private long runtime;

    public DateTime getStartTime() {
        return startTime;
    }

    public void setStartTime(DateTime startTime) {
        this.startTime = startTime;
    }

    public long getRuntime() {
        return runtime;
    }

    public void setRuntime(long runtime) {
        this.runtime = runtime;
    }
}
