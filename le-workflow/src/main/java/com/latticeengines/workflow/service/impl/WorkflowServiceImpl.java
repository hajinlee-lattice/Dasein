package com.latticeengines.workflow.service.impl;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.oozie.client.OozieClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.workflow.service.WorkflowContext;
import com.latticeengines.workflow.service.WorkflowService;

@Component
public class WorkflowServiceImpl implements WorkflowService {

    private final Log log = LogFactory.getLog(this.getClass());

    @Value("${oozie.admin.url}")
    private String oozieAdminUrl;

    @Value("${oozie.workflow.app.path}")
    private String workflowAppPath;

    @Value("${dataplatform.fs.defaultFS}")
    private String nameNode;

    @Value("${dataplatform.yarn.resourcemanager.address}")
    private String jobTracker;

    @Value("${dataplatform.customer.basedir}")
    private String dataplatformCustomerBaseDir;

    @Value("${oozie.workflow.mapred.job.queueName}")
    private String queueName;

    @Override
    public void run(WorkflowContext context) {

        OozieClient wc = new OozieClient(oozieAdminUrl);

        Properties conf = wc.createConfiguration();
        conf.setProperty(OozieClient.APP_PATH, nameNode + workflowAppPath);
//        conf.setProperty(OozieClient.COORDINATOR_APP_PATH, nameNode + workflowAppPath);
        conf.setProperty("workflowAppUri", nameNode + workflowAppPath);

        conf.setProperty("start", "2014-01-01T00:00Z");
        conf.setProperty("end", "2019-01-01T00:00Z");
        
        conf.setProperty("nameNode", nameNode);
        conf.setProperty("jobTracker", jobTracker);
        conf.setProperty("queueName", queueName);
        conf.setProperty("dataplatformCustomerBaseDir", dataplatformCustomerBaseDir);

        Map<String, Object> map = context.getMap();
        for (Entry<String, Object> entry : map.entrySet()) {
            conf.setProperty(entry.getKey(), entry.getValue().toString());
        }
        try {
            String jobId = wc.run(conf);
            log.info("Workflow job submitted, Job Id=" + jobId);

        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

    }
}
