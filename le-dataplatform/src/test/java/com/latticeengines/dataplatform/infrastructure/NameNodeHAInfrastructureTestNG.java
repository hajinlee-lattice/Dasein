package com.latticeengines.dataplatform.infrastructure;

import static org.testng.Assert.assertTrue;

import org.apache.commons.exec.ExecuteException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.annotations.Test;

public class NameNodeHAInfrastructureTestNG extends DataPlatformInfrastructureTestNGBase {

    protected static final Log log = LogFactory.getLog(NameNodeHAInfrastructureTestNG.class);

    private String ACTIVE = "active";

    @Test(groups = "infrastructure", enabled = true)
    public void testConsistentReadFromBothNameNodes() throws Exception {
        String hdfs = "/usr/bin/hdfs ";
        String directoryListPrimary = "";
        String directoryListSecondary = "";
        String result = "";

        try {
            log.info("Force failover from nn2 to nn1");
            result = executeCommand(hdfs + "haadmin -failover nn2 nn1");
            log.info(result);

            log.info("Confirm Active NN is nn1");
            result = executeCommand(hdfs + "haadmin -getServiceState nn1");
            assertTrue(result.trim().contains(ACTIVE));

            log.info("Get directory list from nn1");
            result = executeCommand(hdfs + "hadoop fs -ls -R /app");
            directoryListPrimary = result.substring(result.indexOf("/app"));
            log.info("directoryListPrimary: " + directoryListPrimary);

            log.info("Failover nn1 to nn2");
            result = executeCommand(hdfs + "haadmin -failover nn1 nn2");
            log.info(result);

            log.info("Confirm Active NN is nn2");
            result = executeCommand(hdfs + "haadmin -getServiceState nn2");
            assertTrue(result.trim().contains(ACTIVE));

            log.info("Get directory list from nn2");
            result = executeCommand(hdfs + "dfs -ls -R /app");
            directoryListSecondary = result.substring(result.indexOf("/app"));
            log.info("directoryListSecondary: " + directoryListSecondary);

            log.info("Failover from nn2 to nn1");
            result = executeCommand(hdfs + "haadmin -failover nn2 nn1");
            log.info(result);

            log.info("Confirm Active NN is nn1");
            result = executeCommand(hdfs + "haadmin -getServiceState nn1");
            assertTrue(result.trim().contains(ACTIVE));

            log.info("Compare directory lists");
            assertTrue(directoryListPrimary.equals(directoryListSecondary));
            log.info("Directory lists are equivalent");
        } catch (ExecuteException e) {
            log.error("Result is (" + result + ")");
            throw e;
        }
    }

}
