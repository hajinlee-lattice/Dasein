package org.apache.hadoop.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.ClientCache;
import org.apache.hadoop.mapred.ResourceMgrDelegate;
import org.apache.hadoop.mapred.YARNRunner;
import org.apache.hadoop.mapreduce.v2.app.MRAppMaster;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;

public class LedpYarnRunner extends YARNRunner {

    public LedpYarnRunner(Configuration conf) {
        super(conf);
    }

    public LedpYarnRunner(Configuration conf, ResourceMgrDelegate resMgrDelegate) {
        super(conf, resMgrDelegate, new ClientCache(conf, resMgrDelegate));
    }

    public LedpYarnRunner(Configuration conf, ResourceMgrDelegate resMgrDelegate, ClientCache clientCache) {
        super(conf, resMgrDelegate, clientCache);
    }

    public ApplicationSubmissionContext createApplicationSubmissionContext(Configuration jobConf, String jobSubmitDir,
            Credentials ts) throws IOException {
        ApplicationSubmissionContext appCtx = super.createApplicationSubmissionContext(jobConf, jobSubmitDir, ts);
        ContainerLaunchContext ctrLaunchCtx = appCtx.getAMContainerSpec();
        List<String> commands = ctrLaunchCtx.getCommands();
        List<String> replacement = new ArrayList<>();
        String mrAppMasterClassName = MRAppMaster.class.getName();
        String mrAppMasterReplacementClassName = jobConf.get("yarn.mr.am.class.name");
        
        if (mrAppMasterReplacementClassName != null) {
            try {
                Class.forName(mrAppMasterReplacementClassName);
            } catch (Exception e) {
                return appCtx;
            }
            for (String command : commands) {
                if (command.contains(mrAppMasterClassName)) {
                    command = command.replace(mrAppMasterClassName, mrAppMasterReplacementClassName);
                }
                replacement.add(command);
            }
            ctrLaunchCtx.setCommands(replacement);
        }
        return appCtx;
    }
    
}
