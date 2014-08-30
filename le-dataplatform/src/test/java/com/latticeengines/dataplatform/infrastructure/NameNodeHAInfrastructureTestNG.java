package com.latticeengines.dataplatform.infrastructure;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.commons.exec.ExecuteException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.common.base.Strings;

public class NameNodeHAInfrastructureTestNG extends DataPlatformInfrastructureTestNGBase {

    protected static final Log log = LogFactory.getLog(NameNodeHAInfrastructureTestNG.class);

    private String ACTIVE = "active";

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
    }

    @Test(groups = "infrastructure", enabled = true)
    public void testConsistentReadFromBothNameNodes() throws Exception {
        String hdfs = "/usr/bin/hdfs ";
        String primaryLog = "~/dfs-primary-lsr-1.log";
        String secondaryLog = "~/dfs-secondary-lsr-1.log";
        String result = "";

        try {
        // Remove logs from ~/
        result = executeCommand("rm -rf " + primaryLog);
        result = executeCommand("rm -rf " + secondaryLog);

        // Force failover from nn2 to nn1
        result = executeCommand(hdfs + "haadmin -failover nn2 nn1");

        // Confirm Active NN is nn1
        result = executeCommand(hdfs + "haadmin -getServiceState nn1");
        assertEquals(result, ACTIVE);
        // Get log from nn1
        result = executeCommand(hdfs + "dfs -ls -R / > " + primaryLog);

        // Failover nn1 to nn2
        result = executeCommand(hdfs + "haadmin -failover nn1 nn2");

        // Confirm Active NN is nn2
        result = executeCommand(hdfs + "haadmin -getServiceState nn2");
        assertEquals(result, ACTIVE);

        // Get log from nn2
        result = executeCommand(hdfs + "dfs -ls -R / > " + secondaryLog);

        // Failover from nn2 to nn1
        result = executeCommand(hdfs + "haadmin -failover nn2 nn1");

        // Confirm Active NN is nn1
        result = executeCommand(hdfs + "haadmin -getServiceState nn1");
        assertEquals(result, ACTIVE);

        // Diff logs
        result = executeCommand("diff " + primaryLog + " " + secondaryLog);
        assertTrue(Strings.isNullOrEmpty(result));
        } catch (ExecuteException e) {
            System.out.println(result);
            log.error(result);
            throw e;
        }
    }

}
