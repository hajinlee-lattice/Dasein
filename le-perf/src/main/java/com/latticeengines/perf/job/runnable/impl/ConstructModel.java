package com.latticeengines.perf.job.runnable.impl;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.latticeengines.perf.job.configuration.ConstructModelConfiguration;
import com.latticeengines.perf.job.runnable.ModelingResourceJob;

public class ConstructModel extends ModelingResourceJob<ConstructModelConfiguration, List<String>> {

    private OnBoard ob;

    private SubmitModel sm;

    public ConstructModel(Configuration hadoopConfiguration, String hdfsPath) {
        this.ob = new OnBoard(hadoopConfiguration, hdfsPath);
        this.sm = new SubmitModel();
    }

    @Override
    public List<String> executeJob() throws Exception {
        return constructModel();
    }

    @Override
    public void setConfiguration(String restEndpointHost, ConstructModelConfiguration config) throws Exception {
        this.restEndpointHost = restEndpointHost;
        ob.setConfiguration(restEndpointHost, config.getOnBoardConfiguration());
        sm.setConfiguration(restEndpointHost, config.getModel());
    }

    public List<String> constructModel() throws Exception {
        List<String> appIds = ob.executeJob();
        GetJobStatus.checkStatus(restEndpointHost, appIds);
        appIds = sm.executeJob();
        GetJobStatus.checkStatus(restEndpointHost, appIds);
        return appIds;
    }
}
