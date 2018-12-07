package com.latticeengines.serviceflows.functionalframework;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.domain.exposed.serviceflows.core.spark.WorkflowSparkJobConfig;
import com.latticeengines.domain.exposed.spark.ScriptJobConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.SparkScript;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;


@DirtiesContext
@ContextConfiguration(locations = { "classpath:test-serviceflows-spark-context.xml" })
public class ServiceFlowsSparkJobFunctionalTestNGBase extends SparkJobFunctionalTestNGBase {

    @Inject
    private VersionManager versionManager;

    @Value("${dataplatform.hdfs.stack}")
    private String stackName;

    @BeforeClass(groups = "functional")
    public void setup() {
        setupLivyEnvironment();
    }

    @AfterClass(groups = "functional", alwaysRun = true)
    public void teardown() {
        tearDownLivyEnvironment();
    }

    protected <J extends AbstractSparkJob<C>, C extends WorkflowSparkJobConfig> //
    SparkJobResult runSparkJob(Class<J> jobClz, C jobConfig) {
        jobConfig.setWorkspace(getWorkspace());
        return super.runSparkJob(jobClz, jobConfig, getSwLibJars());
    }

    @Override
    protected SparkJobResult runSparkScript(SparkScript script, ScriptJobConfig jobConfig) {
        return super.runSparkScript(script, jobConfig);
    }

    protected List<String> getSwLibs() {
        return Collections.singletonList("scoring");
    }

    private Iterable<String> getSwLibJars() {
        return () -> new Iterator<String>() {
            private final Iterator<String> itr = getSwLibs().iterator();
            private final String version = versionManager.getCurrentVersion();

            @Override
            public boolean hasNext() {
                return itr.hasNext();
            }

            @Override
            public String next() {
                String swlib = itr.next();
                return "/app/" + stackName + "/" + version //
                        + "/swlib/dataflowapi/le-serviceflows-" + swlib //
                        + "/le-serviceflows-" + swlib + ".jar";
            }
        };
    }

}
