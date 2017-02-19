package com.latticeengines.flink;

import static com.latticeengines.flink.FlinkConstants.AM_JAR;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.yarn.AbstractYarnClusterDescriptor;
import org.apache.flink.yarn.YarnClusterClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-flink-context.xml" })
public class FlinkYarnClusterTestNG extends AbstractTestNGSpringContextTests {

    @Autowired
    private YarnConfiguration yarnConfiguration;

    @Autowired
    private VersionManager versionManager;

    @Value("${dataplatform.queue.scheme:legacy}")
    private String queueScheme;

    @Test(groups = "functional")
    public void testDeployCluster() throws Exception {
        String queue = LedpQueueAssigner.getModelingQueueNameForSubmission();
        queue = LedpQueueAssigner.overwriteQueueAssignment(queue, queueScheme);
        AbstractYarnClusterDescriptor yarnDescriptor = FlinkYarnCluster.createDescriptor(yarnConfiguration,
                new Configuration(), "", queue);
        Map<String, String> flinkDistJar = new HashMap<>();
        flinkDistJar.put(AM_JAR, getDataflowJarPath());
        yarnDescriptor.setExtraJars(flinkDistJar);
        FlinkYarnCluster.launch(yarnDescriptor);
        YarnClusterClient clusterClient = FlinkYarnCluster.getClient();
        Assert.assertNotNull(clusterClient);
        FlinkYarnCluster.getExecutionEnvironment();
        FlinkYarnCluster.shutdown();
    }

    private String getDataflowJarPath() throws Exception {
        String artifactVersion = versionManager.getCurrentVersion();
        String dataFlowLibDir = StringUtils.isEmpty(artifactVersion) ? "/app/dataflow/lib/"
                : "/app/" + artifactVersion + "/dataflow/lib/";
        List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, dataFlowLibDir);
        for (String file : files) {
            if (file.contains("le-dataflow-") && file.endsWith(".jar")) {
                return file;
            }
        }
        return "/app/flink/flink.jar";
    }

}
