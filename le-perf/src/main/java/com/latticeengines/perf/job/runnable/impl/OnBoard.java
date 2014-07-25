package com.latticeengines.perf.job.runnable.impl;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

import com.latticeengines.perf.job.configuration.OnBoardConfiguration;
import com.latticeengines.perf.job.runnable.ModelingResourceJob;

public class OnBoard extends ModelingResourceJob<OnBoardConfiguration, List<String>> {

    private LoadData ld;
    private CreateSamples cs;
    private Profile pf;
    private Configuration hadoopConfiguration;
    private String hdfsPath;

    public OnBoard(Configuration hadoopConfiguration, String hdfsPath) {
        ld = new LoadData();
        cs = new CreateSamples();
        pf = new Profile();
        this.hadoopConfiguration = hadoopConfiguration;
        this.hdfsPath = hdfsPath;
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
        GetJobStatus.checkStatus(restEndpointHost, appIds);
        while (!GetJobStatus.validateFiles(hadoopConfiguration, hdfsPath + "/data/Q_EventTable_Nutanix", 4)) {
        }
        appIds = cs.executeJob();
        GetJobStatus.checkStatus(restEndpointHost, appIds);
        appIds = pf.executeJob();
        GetJobStatus.checkStatus(restEndpointHost, appIds);
        return appIds;
    }
}
