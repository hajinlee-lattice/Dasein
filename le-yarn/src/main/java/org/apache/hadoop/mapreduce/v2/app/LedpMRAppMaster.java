package org.apache.hadoop.mapreduce.v2.app;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.util.Clock;

public class LedpMRAppMaster extends MRAppMaster {

    public LedpMRAppMaster(ApplicationAttemptId applicationAttemptId,
            ContainerId containerId, String nmHost, int nmPort, int nmHttpPort,
            long appSubmitTime) {
        super(applicationAttemptId, containerId, nmHost, nmPort, nmHttpPort, appSubmitTime);
    }
    
    public LedpMRAppMaster(ApplicationAttemptId applicationAttemptId, ContainerId containerId, String nmHost,
            int nmPort, int nmHttpPort, Clock clock, long appSubmitTime) {
        super(applicationAttemptId, containerId, nmHost, nmPort, nmHttpPort, clock, appSubmitTime);
    }

    public static void main(String[] args) {
        MRAppMaster.main(args);
    }
}
