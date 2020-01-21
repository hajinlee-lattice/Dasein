package com.latticeengines.modelquality.functionalframework;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.modelquality.entitymgr.AlgorithmEntityMgr;
import com.latticeengines.modelquality.entitymgr.AnalyticPipelineEntityMgr;
import com.latticeengines.modelquality.entitymgr.AnalyticTestEntityMgr;
import com.latticeengines.modelquality.entitymgr.DataFlowEntityMgr;
import com.latticeengines.modelquality.entitymgr.DataSetEntityMgr;
import com.latticeengines.modelquality.entitymgr.ModelConfigEntityMgr;
import com.latticeengines.modelquality.entitymgr.ModelRunEntityMgr;
import com.latticeengines.modelquality.entitymgr.PipelineEntityMgr;
import com.latticeengines.modelquality.entitymgr.PipelineStepEntityMgr;
import com.latticeengines.modelquality.entitymgr.PipelineToPipelineStepsEntityMgr;
import com.latticeengines.modelquality.entitymgr.PropDataEntityMgr;
import com.latticeengines.modelquality.entitymgr.SamplingEntityMgr;
import com.latticeengines.modelquality.service.AnalyticPipelineService;
import com.latticeengines.modelquality.service.AnalyticTestService;
import com.latticeengines.modelquality.service.PipelineService;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-modelquality-context.xml" })
public class ModelQualityTestNGBase extends AbstractTestNGSpringContextTests {

    @Value("${modelquality.file.upload.hdfs.dir}")
    protected String hdfsDir;

    @Inject
    protected Configuration yarnConfiguration;

    @Inject
    protected AlgorithmEntityMgr algorithmEntityMgr;
    @Inject
    protected DataFlowEntityMgr dataFlowEntityMgr;
    @Inject
    protected DataSetEntityMgr dataSetEntityMgr;
    @Inject
    protected PipelineEntityMgr pipelineEntityMgr;
    @Inject
    protected PipelineStepEntityMgr pipelineStepEntityMgr;
    @Inject
    protected PipelineToPipelineStepsEntityMgr pipelineToPipelineStepsEntityMgr;
    @Inject
    protected PropDataEntityMgr propDataEntityMgr;
    @Inject
    protected ModelRunEntityMgr modelRunEntityMgr;
    @Inject
    protected ModelConfigEntityMgr modelConfigEntityMgr;
    @Inject
    protected SamplingEntityMgr samplingEntityMgr;
    @Inject
    protected AnalyticPipelineEntityMgr analyticPipelineEntityMgr;
    @Inject
    protected AnalyticTestEntityMgr analyticTestEntityMgr;
    @Inject
    protected PipelineService pipelineService;
    @Inject
    protected AnalyticPipelineService analyticPipelineService;
    @Inject
    protected AnalyticTestService analyticTestService;

    protected void cleanupHdfs() throws Exception {
        HdfsUtils.rmdir(yarnConfiguration, hdfsDir + "/steps");
        HdfsUtils.rmdir(yarnConfiguration, hdfsDir + "/pipelines");
    }
}
