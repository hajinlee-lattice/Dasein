package com.latticeengines.perf.job.runnable.impl;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFilenameFilter;
import com.latticeengines.common.exposed.util.YarnUtils;
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
                log.info(appId + " state is: " + state + " status is :" + status);
                if (status.equals(FinalApplicationStatus.SUCCEEDED)) {
                    appIds.remove(appId);
                    i--;
                } else if (status.equals(FinalApplicationStatus.FAILED) && !YarnUtils.isPrempted(gs.getDiagnostics())
                        || status.equals(FinalApplicationStatus.KILLED)) {
                    return false;
                }
            }
            Thread.sleep(6000L);
        }
        Thread.sleep(6000L);
        return true;
    }

    public static boolean validateFiles(Configuration hadoopConfiguration, String hdfsPath, int numOfAvros)
            throws Exception {
        List<String> files = HdfsUtils.getFilesForDir(hadoopConfiguration, hdfsPath, new HdfsFilenameFilter() {
            @Override
            public boolean accept(String filename) {
                return filename.endsWith(".avro");
            }

        });
        if (files.size() == numOfAvros) {
            log.info(StringUtils.join(", ", files));
            return true;
        }
        return false;
    }
}
