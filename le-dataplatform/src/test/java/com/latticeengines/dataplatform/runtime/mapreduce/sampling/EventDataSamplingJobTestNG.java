package com.latticeengines.dataplatform.runtime.mapreduce.sampling;

import java.util.Properties;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.ProxyUtils;
import com.latticeengines.dataplatform.exposed.service.ModelingService;
import com.latticeengines.dataplatform.functionalframework.DataplatformMiniClusterFunctionalTestNG;
import com.latticeengines.dataplatform.service.impl.ModelingServiceImpl;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.domain.exposed.modeling.SamplingElement;

public class EventDataSamplingJobTestNG extends DataplatformMiniClusterFunctionalTestNG {

    @Autowired
    private ModelingService modelingService;

    private String input = ClassLoader
            .getSystemResource("com/latticeengines/dataplatform/runtime/mapreduce/DELL_EVENT_TABLE").getPath();

    private static final String CUSTOMER = EventDataSamplingJobTestNG.class.getSimpleName();

    private static final String EVENT_TABLE_NAME = "DELL_EVENT_TABLE";

    @Test(groups = "functional")
    public void test() throws Exception {
        SamplingConfiguration samplingConfig = new SamplingConfiguration();
        samplingConfig.setCustomer(CUSTOMER);
        samplingConfig.setTrainingPercentage(80);
        samplingConfig.setTable(EVENT_TABLE_NAME);
        SamplingElement s0 = new SamplingElement();
        s0.setName("s0");
        s0.setPercentage(30);
        SamplingElement s1 = new SamplingElement();
        s1.setName("s1");
        s1.setPercentage(60);
        samplingConfig.addSamplingElement(s0);
        samplingConfig.addSamplingElement(s1);
        String targetPath = customerBaseDir + "/" + CUSTOMER + "/data/" + EVENT_TABLE_NAME;
        if (HdfsUtils.fileExists(miniclusterConfiguration, targetPath)) {
            HdfsUtils.rmdir(miniclusterConfiguration, targetPath);
        }
        HdfsUtils.copyFromLocalToHdfs(miniclusterConfiguration, input, targetPath);

        Properties properties = ((ModelingServiceImpl) ProxyUtils.getTargetObject(modelingService))
                .customSamplingConfig(samplingConfig);

        testMRJob(EventDataSamplingJob.class, properties);

    }
}
