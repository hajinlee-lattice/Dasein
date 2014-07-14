package com.latticeengines.perf.job.runnable.impl;

import java.util.List;

import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;

import com.latticeengines.perf.job.configuration.OnBoardConfiguration;
import com.latticeengines.perf.job.runnable.ModelingResourceJob;

public class OnBoard extends ModelingResourceJob<OnBoardConfiguration, List<String>> {

    private LoadData ld;
    private CreateSamples cs;
    private Profile pf;

    public OnBoard() {
        ld = new LoadData();
        cs = new CreateSamples();
        pf = new Profile();
    }

    @Override
    public void setConfiguration(String restEndpointHost, OnBoardConfiguration config) throws Exception {
        this.restEndpointHost = restEndpointHost;
        ld.setConfiguration(restEndpointHost, config.getLoadConfiguration());
        cs.setConfiguration(restEndpointHost, config.getSamplingConfiguration());
        pf.setConfiguration(restEndpointHost, config.getDataProfileConfiguration());
    }

    @Override
    public List<String> executeJob() throws Exception {
        return onBoard();
    }

    public List<String> onBoard() throws Exception {
        List<String> appIds = ld.executeJob();
        checkStatus(restEndpointHost, appIds);
        appIds = cs.executeJob();
        checkStatus(restEndpointHost, appIds);
        appIds = pf.executeJob();
        checkStatus(restEndpointHost, appIds);
        return appIds;
    }

    public boolean checkStatus(String restEndpointHost, List<String> appIds) throws Exception {
        while (appIds.size() > 0) {
            for (String appId : appIds) {
                GetJobStatus gjs = new GetJobStatus();
                gjs.setConfiguration(restEndpointHost, appId);
                FinalApplicationStatus state = gjs.getJobStatus().getState();
                if (state.equals(FinalApplicationStatus.SUCCEEDED)) {
                    appIds.remove(appId);
                } else if (!state.equals(FinalApplicationStatus.UNDEFINED)) {
                    return false;
                }
            }
            Thread.sleep(10000L);
        }
        return true;
    }
}
