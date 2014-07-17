package com.latticeengines.perf.job.runnable.impl;

import java.util.List;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.perf.job.runnable.ModelingResourceJob;

public class GetJobStatus extends ModelingResourceJob<String, JobStatus> {

    @Override
    public JobStatus executeJob() throws Exception {
        return getJobStatus();
    }

    public JobStatus getJobStatus() throws Exception {
        JobStatus js = rc.getJobStatus(config);
        log.info("Application " + js.getId() + " is in progress " + js.getProgress());
        return js;
    }

    public static boolean checkStatus(String restEndpointHost, List<String> appIds) throws Exception {
        while (appIds.size() > 0) {
            for (int i = 0; i < appIds.size(); i++) {
                String appId = appIds.get(i);
                GetJobStatus gjs = new GetJobStatus();
                gjs.setConfiguration(restEndpointHost, appId);
                JobStatus gs = gjs.getJobStatus();
                FinalApplicationStatus status = gs.getStatus();
                YarnApplicationState state = gs.getState();
                if (state.equals(YarnApplicationState.FINISHED) && status.equals(FinalApplicationStatus.SUCCEEDED)) {
                    appIds.remove(appId);
                    i--;
                } else if (state.equals(YarnApplicationState.FINISHED) && status.equals(FinalApplicationStatus.FAILED)) {
                    return false;
                }
            }
            Thread.sleep(5000L);
        }
        return true;
    }
}
