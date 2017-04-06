package com.latticeengines.dataplatform.runtime.mapreduce.sampling;

import java.util.Properties;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.exposed.mapreduce.MRJobUtil;
import com.latticeengines.dataplatform.exposed.mapreduce.MapReduceProperty;
import com.latticeengines.dataplatform.functionalframework.DataplatformMiniClusterFunctionalTestNG;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.domain.exposed.modeling.SamplingElement;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

public class EventDataSamplingJobTestNG extends DataplatformMiniClusterFunctionalTestNG {

    private String input = ClassLoader
            .getSystemResource("com/latticeengines/dataplatform/runtime/mapreduce/DELL_EVENT_TABLE").getPath();

    @Test(groups = "functional")
    public void test() throws Exception {
        SamplingConfiguration samplingConfig = new SamplingConfiguration();
        samplingConfig.setTrainingPercentage(80);
        SamplingElement s0 = new SamplingElement();
        s0.setName("s0");
        s0.setPercentage(30);
        SamplingElement s1 = new SamplingElement();
        s1.setName("s1");
        s1.setPercentage(60);
        samplingConfig.addSamplingElement(s0);
        samplingConfig.addSamplingElement(s1);
        HdfsUtils.copyFromLocalToHdfs(miniclusterConfiguration, input, "/DELL_EVENT_TABLE");

        Properties properties = new Properties();
        properties.setProperty(MapReduceProperty.INPUT.name(), "/DELL_EVENT_TABLE");
        properties.setProperty(MapReduceProperty.OUTPUT.name(), "/samples");
        properties.setProperty(MapReduceProperty.CUSTOMER.name(), "Dell");
        properties.setProperty(MapReduceProperty.QUEUE.name(), LedpQueueAssigner.getModelingQueueNameForSubmission());
        properties.setProperty(EventDataSamplingProperty.SAMPLE_CONFIG.name(), samplingConfig.toString());
        properties.setProperty(MapReduceProperty.CACHE_FILE_PATH.name(), MRJobUtil.getPlatformShadedJarPath(
                miniclusterConfiguration, versionManager.getCurrentVersionInStack(stackName)));

        testMRJob(EventDataSamplingJob.class, properties);

    }
}
