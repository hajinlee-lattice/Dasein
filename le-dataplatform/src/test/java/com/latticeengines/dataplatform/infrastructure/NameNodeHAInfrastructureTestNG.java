package com.latticeengines.dataplatform.infrastructure;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

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

        // Remove logs from ~/
        executeCommand("rm " + primaryLog);
        executeCommand("rm " + secondaryLog);

        // Force failover from nn2 to nn1
        executeCommand(hdfs + "haadmin -failover nn2 nn1");

        // Confirm Active NN is nn1
        String state = executeCommand(hdfs + "haadmin -getServiceState nn1");
        assertEquals(state, ACTIVE);

        // Get log from nn1
        executeCommand(hdfs + "dfs -ls -R / > " + primaryLog);

        // Failover nn1 to nn2
        executeCommand(hdfs + "haadmin -failover nn1 nn2");

        // Confirm Active NN is nn2
        state = executeCommand(hdfs + "haadmin -getServiceState nn2");
        assertEquals(state, ACTIVE);

        // Get log from nn2
        executeCommand(hdfs + "dfs -ls -R / > " + secondaryLog);

        // Failover from nn2 to nn1
        executeCommand(hdfs + "haadmin -failover nn2 nn1");

        // Confirm Active NN is nn1
        state = executeCommand(hdfs + "haadmin -getServiceState nn1");
        assertEquals(state, ACTIVE);

        // Diff logs
        String diff = executeCommand("diff " + primaryLog + " " + secondaryLog);
        assertTrue(Strings.isNullOrEmpty(diff));
    }

}
